"""The Parquet archive and the rollup rebuild, against real ClickHouse and a real S3 API.

ClickHouse reaches S3 by the URL in ARCHIVE_TEST_URL, as ClickHouse sees it: SeaweedFS on
the same Docker network (`make test-integration` starts it). Each test writes under its
own prefix.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from livedemos.archive.job import HOUR_S, Archiver, hour_url, s3_function
from livedemos.archive.rebuild import ArchiveIncomplete, rebuild
from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import ArchiveSettings

from .conftest import clickhouse_test_settings, rows, user_settings

pytestmark = pytest.mark.integration

NOW = datetime.now(UTC)
# Three hours on one UTC day, two days ago: inside the lookback, outside any settle window.
DAY = (NOW - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
H0, H1, H2 = (int(DAY.timestamp()) + h * HOUR_S for h in (3, 4, 5))


@pytest.fixture
def settings() -> ArchiveSettings:
    base = os.environ.get("ARCHIVE_TEST_URL", "http://s3:8333/archive/test")
    return ArchiveSettings(url=f"{base}/{uuid4().hex}", nosign=True)


@pytest.fixture
async def edits(ch: ClickHouse) -> ClickHouse:
    """50, 40 and 30 edits in H0, H1 and H2, and one just now, so ingest is past them all."""
    seq = 1
    for hour_s, n in ((H0, 50), (H1, 40), (H2, 30)):
        start = datetime.fromtimestamp(hour_s + 10, UTC)
        await ch.insert("wiki_edits", rows(n, start=start, bot_every=4, first_seq=seq))
        seq += n
    await ch.insert("wiki_edits", rows(1, first_seq=seq))
    return ch


async def file_count(ch: ClickHouse, settings: ArchiveSettings, hour_s: int) -> int:
    result = await ch.query(
        f"SELECT count() AS n FROM {s3_function(settings, 'Parquet')}",
        params={"url": hour_url(settings.url, hour_s)},
        settings={"use_hive_partitioning": "0"},
    )
    return int(result.rows[0]["n"])


async def per_minute(ch: ClickHouse) -> list[tuple[str, str, int, int]]:
    result = await ch.query(
        "SELECT toString(minute) AS m, lang, sum(edits) AS e, sum(bot_edits) AS b "
        "FROM wiki_edits_per_minute WHERE toDate(minute) = {day:Date} "
        "GROUP BY minute, lang ORDER BY minute, lang",
        params={"day": DAY.date().isoformat()},
    )
    return [(r["m"], r["lang"], int(r["e"]), int(r["b"])) for r in result.rows]


async def test_each_finished_hour_becomes_one_file_with_every_row(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    archiver = Archiver(settings, edits)
    results = await archiver.run_once(NOW)

    written = {r.hour_s: r.rows for r in results if r.result == "written"}
    assert written == {H0: 50, H1: 40, H2: 30}
    assert {r.result for r in results} <= {"written", "empty"}  # the hours in between
    assert await archiver.existing_hours([H0, H1, H2]) == {H0, H1, H2}
    for hour_s, n in written.items():
        assert await file_count(edits, settings, hour_s) == n

    # Nothing left to do: a second run writes nothing.
    again = await archiver.run_once(NOW)
    assert [r for r in again if r.result != "empty"] == []


async def test_an_hour_ingest_hasnt_passed_yet_waits(
    ch: ClickHouse, settings: ArchiveSettings
) -> None:
    await ch.insert("wiki_edits", rows(10, start=datetime.fromtimestamp(H0 + 10, UTC)))
    # Newest event 2 minutes after H0 ends: inside the 5-minute settle window.
    await ch.insert(
        "wiki_edits", rows(1, start=datetime.fromtimestamp(H0 + HOUR_S + 120, UTC), first_seq=11)
    )
    plan = await Archiver(settings, ch).plan(NOW)
    assert H0 not in plan.due


async def test_missed_hours_are_written_on_the_next_run(
    ch: ClickHouse, settings: ArchiveSettings
) -> None:
    archiver = Archiver(settings, ch)
    await ch.insert("wiki_edits", rows(10, start=datetime.fromtimestamp(H0 + 10, UTC)))
    await ch.insert(
        "wiki_edits", rows(1, start=datetime.fromtimestamp(H0 + HOUR_S + 60, UTC), first_seq=11)
    )
    assert (await archiver.plan(NOW)).due == []  # H0 not yet passed by settle_s

    # Ingest catches up (a replay after an outage, say) far past H0 and H1.
    await ch.insert(
        "wiki_edits", rows(20, start=datetime.fromtimestamp(H1 + 10, UTC), first_seq=12)
    )
    await ch.insert("wiki_edits", rows(1, first_seq=40))
    results = {r.hour_s: r for r in await archiver.run_once(NOW)}
    assert (results[H0].result, results[H0].rows) == ("written", 10)
    assert (results[H1].result, results[H1].rows) == ("written", 21)


async def test_an_existing_file_is_never_replaced_by_the_scheduled_run(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    """A host rebuilt from scratch has fewer raw rows than the files its predecessor wrote."""
    archiver = Archiver(settings, edits)
    await archiver.run_once(NOW)
    await edits.execute(
        "ALTER TABLE wiki_edits DELETE WHERE event_time < fromUnixTimestamp({to_s:Int64}) "
        "SETTINGS mutations_sync = 1",
        params={"to_s": H0 + 30},
    )
    await archiver.run_once(NOW)
    assert await file_count(edits, settings, H0) == 50

    # Rewriting an hour is deliberate: archive_hour (the --hour flag) replaces the file.
    rewritten = await archiver.archive_hour(H0)
    assert (rewritten.result, rewritten.rows) == ("written", 30)
    assert await file_count(edits, settings, H0) == 30


async def test_rebuild_drill_restores_the_rollup_from_the_archive(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    await Archiver(settings, edits).run_once(NOW)
    before = await per_minute(edits)
    assert sum(e for _, _, e, _ in before) == 120

    await edits.execute("TRUNCATE TABLE wiki_edits_per_minute")
    assert await per_minute(edits) == []
    rollup_rows = await rebuild(
        edits, settings, DAY.date(), DAY.date() + timedelta(days=1), quiet=timedelta(0)
    )

    assert await per_minute(edits) == before
    assert rollup_rows == len(before)


class DedupEverything(ClickHouse):
    """A server whose default profile turns INSERT ... SELECT deduplication on: the
    settings the code passes still apply on top."""

    async def execute(self, sql: str, *, params: Any = None, settings: Any = None) -> None:
        forced = {"deduplicate_insert_select": "enable_even_for_bad_queries"}
        await super().execute(sql, params=params, settings={**forced, **(settings or {})})


async def test_a_rebuild_twice_is_still_right(edits: ClickHouse, settings: ArchiveSettings) -> None:
    """Each rebuild's block matches one ClickHouse has seen (the view's, then the first
    rebuild's). With INSERT ... SELECT dedup on, it would be dropped after the DELETE."""
    await Archiver(settings, edits).run_once(NOW)
    before = await per_minute(edits)
    forced = DedupEverything(clickhouse_test_settings())
    try:
        for _ in range(2):
            await rebuild(
                forced, settings, DAY.date(), DAY.date() + timedelta(days=1), quiet=timedelta(0)
            )
    finally:
        await forced.aclose()
    assert await per_minute(edits) == before


async def test_rebuild_refuses_when_the_archive_is_missing_hours(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    await Archiver(settings, edits).archive_hour(H0)  # H1 and H2 never archived
    before = await per_minute(edits)
    with pytest.raises(ArchiveIncomplete, match="2 hour"):
        await rebuild(
            edits, settings, DAY.date(), DAY.date() + timedelta(days=1), quiet=timedelta(0)
        )
    assert await per_minute(edits) == before  # untouched


@pytest.fixture
async def archiver_user(settings: ArchiveSettings) -> AsyncIterator[ClickHouse]:
    client = ClickHouse(user_settings("archiver", "CLICKHOUSE_ARCHIVER_PASSWORD"))
    yield client
    await client.aclose()


@pytest.mark.usefixtures("demos_schema")
async def test_the_archiver_user_can_do_its_job(
    archiver_user: ClickHouse, settings: ArchiveSettings
) -> None:
    """As the real user, on `demos`: plan, list and write a file."""
    archiver = Archiver(settings, archiver_user)
    await archiver.plan(NOW)
    await archiver_user.execute(
        f"INSERT INTO FUNCTION {s3_function(settings, 'Parquet')} "
        "SELECT toString(event_id) AS event_id FROM wiki_edits LIMIT 1",
        params={"url": hour_url(settings.url, H0)},
        settings={"use_hive_partitioning": "0", "s3_truncate_on_insert": "1"},
    )
    await archiver.existing_hours([H0])


@pytest.mark.usefixtures("demos_schema")
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count() FROM demos.ingest_gaps",
        "INSERT INTO demos.wiki_edits (title) VALUES ('x')",
        "SELECT * FROM url('http://example.com', 'One')",
        "SELECT * FROM file('anything', 'One')",
        "ALTER TABLE demos.wiki_edits DELETE WHERE 1",
    ],
)
async def test_the_archiver_user_can_do_nothing_else(archiver_user: ClickHouse, sql: str) -> None:
    with pytest.raises(ClickHouseError, match=r"ACCESS_DENIED|Not enough privileges"):
        await archiver_user.execute(sql)
