"""Schema, inserts and queries against a real ClickHouse."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from livedemos.api.activity import ActivityService, parse_request
from livedemos.api.snapshot import Snapshotter
from livedemos.clickhouse import ClickHouse
from livedemos.ingest.events import Edit, to_row
from livedemos.ingest.resume import load_resume_state
from livedemos.migrate import migrate

pytestmark = pytest.mark.integration


def rows(
    n: int, *, lang: str = "en", start: datetime | None = None, bot_every: int = 0
) -> list[dict[str, object]]:
    start = start or datetime.now(UTC) - timedelta(seconds=n)
    out = []
    for i in range(n):
        edit = Edit(
            event_id=str(uuid4()),
            event_time=start + timedelta(seconds=i),
            wiki=f"{lang}wiki",
            lang=lang,
            type="edit",
            namespace=0,
            title=f"Article {i % 3}",
            is_bot=bool(bot_every) and i % bot_every == 0,
        )
        out.append(
            to_row(
                edit,
                sse_id=f'[{{"timestamp":{i}}}]',
                ingest_seq=i + 1,
                ingested_at=datetime.now(UTC),
            )
        )
    return out


async def scalar(ch: ClickHouse, sql: str) -> int:
    result = await ch.query(sql)
    return int(next(iter(result.rows[0].values())))


async def test_migrations_are_idempotent(ch: ClickHouse) -> None:
    await migrate(ch)
    await migrate(ch)
    tables = await ch.query(
        "SELECT name FROM system.tables WHERE database = {db:String} ORDER BY name",
        params={"db": ch.database},
    )
    assert [r["name"] for r in tables.rows] == [
        "ingest_gaps",
        "wiki_edits",
        "wiki_edits_per_minute",
        "wiki_edits_per_minute_mv",
    ]


async def test_a_retried_batch_is_written_once_including_the_rollup(ch: ClickHouse) -> None:
    batch = rows(50)
    await ch.insert("wiki_edits", batch, dedup_token="batch-1")
    await ch.insert("wiki_edits", batch, dedup_token="batch-1")  # timeout-then-retry
    assert await scalar(ch, "SELECT count() FROM wiki_edits") == 50
    assert await scalar(ch, "SELECT sum(edits) FROM wiki_edits_per_minute") == 50


async def test_rollup_matches_raw_per_minute(ch: ClickHouse) -> None:
    for i in range(5):
        await ch.insert("wiki_edits", rows(40, lang=["en", "pt"][i % 2]), dedup_token=f"b{i}")
    mismatches = await scalar(
        ch,
        """
        SELECT count() FROM (
            SELECT toStartOfMinute(event_time) AS minute, lang, count() AS raw
            FROM wiki_edits GROUP BY minute, lang
        ) AS r
        FULL OUTER JOIN (
            SELECT minute, lang, sum(edits) AS rolled
            FROM wiki_edits_per_minute GROUP BY minute, lang
        ) AS m USING (minute, lang)
        WHERE raw != rolled
        """,
    )
    assert mismatches == 0


async def test_resume_state_points_at_the_newest_bookmark(ch: ClickHouse) -> None:
    await ch.insert("wiki_edits", rows(30), dedup_token="b")
    state = await load_resume_state(
        ch,
        now=datetime.now(UTC),
        retention=timedelta(days=7),
        lookback=timedelta(hours=1),
        seam_ids=20_000,
    )
    assert state.bookmark == '[{"timestamp":29}]'  # highest ingest_seq
    assert state.since is None
    assert len(state.seam_ids) == 30


async def test_first_boot_and_too_old_bookmarks(ch: ClickHouse) -> None:
    now = datetime.now(UTC)
    first = await load_resume_state(
        ch,
        now=now,
        retention=timedelta(days=7),
        lookback=timedelta(hours=1),
        seam_ids=20_000,
    )
    assert first.bookmark is None
    assert first.since == now - timedelta(hours=1)

    # Two hours old, with a one-hour retention: the source can't replay that far back.
    await ch.insert("wiki_edits", rows(5, start=now - timedelta(hours=2)), dedup_token="old")
    old = await load_resume_state(
        ch,
        now=now,
        retention=timedelta(hours=1),
        lookback=timedelta(hours=1),
        seam_ids=20_000,
    )
    assert old.bookmark is None
    assert old.since == now
    assert old.gap_from is not None


async def test_snapshot_from_real_rows(ch: ClickHouse) -> None:
    await ch.insert("wiki_edits", rows(100, lang="en", bot_every=4), dedup_token="en")
    await ch.insert("wiki_edits", rows(20, lang="de"), dedup_token="de")
    snapshotter = Snapshotter(ch, langs=["en", "pt", "de"], interval_s=1, stale_after_s=60)
    payload = json.loads((await snapshotter.build()).body)
    assert payload["status"] == "live"
    assert payload["langs"]["en"]["edits_5m"] == 100
    assert payload["langs"]["all"]["edits_5m"] == 120
    assert payload["langs"]["en"]["bot_share_5m"] == 0.25
    assert payload["langs"]["en"]["pages_5m"] == 3
    assert payload["langs"]["all"]["top_articles"][0]["edits"] >= 34
    assert len(payload["langs"]["all"]["per_minute"]) == 60


async def test_query_it_reports_clickhouse_timing(ch: ClickHouse) -> None:
    await ch.insert("wiki_edits", rows(200), dedup_token="x")
    service = ActivityService(ch, ttl_s=10)
    started = time.perf_counter()
    payload = await service.get(parse_request("en", "1h", allowed=["en", "pt", "de"]))
    assert time.perf_counter() - started < 1.0
    assert payload["edits"] == 200
    assert payload["query"]["rows_read"] >= 200
    assert payload["query"]["elapsed_ms"] > 0
