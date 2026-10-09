"""The Ops queries against a real ClickHouse (api/ops.py, migration 0005)."""

from datetime import UTC, datetime, timedelta

import pytest

from livedemos.api import ops
from livedemos.clickhouse import ClickHouse
from tests.integration.conftest import rows

pytestmark = pytest.mark.integration

BASE = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)  # well before anything the view samples


def minute(n: int) -> str:
    return (BASE + timedelta(minutes=n)).isoformat()


async def test_freshness_counts_each_minute_once_and_conservatively(ch: ClickHouse) -> None:
    await ch.insert(
        "freshness_samples",
        [
            {"minute": minute(0), "sampled_at": minute(0), "age_s": 2.5},  # fresh
            {"minute": minute(1), "sampled_at": minute(1), "age_s": 75.0},  # stale
            {"minute": minute(2), "sampled_at": minute(2), "age_s": None},  # no rows: stale
            # minute 3: no sample at all
            {"minute": minute(4), "sampled_at": minute(4), "age_s": 1.0},  # sampled twice,
            {"minute": minute(4), "sampled_at": minute(4), "age_s": 61.0},  # once stale
            {"minute": minute(5), "sampled_at": minute(5), "age_s": 59.9},  # fresh
        ],
    )
    start = int(BASE.timestamp())
    result = await ch.query(
        ops.FRESHNESS, params={"threshold_s": 60.0, "start_s": start, "end_s": start + 6 * 60}
    )
    assert result.rows == [{"sampled": 5, "fresh": 2}]


async def test_clickhouse_samples_freshness_by_itself(ch: ClickHouse) -> None:
    await ch.insert("wiki_edits", rows(5))
    await ch.execute("SYSTEM REFRESH VIEW freshness_samples_mv")
    await ch.execute("SYSTEM WAIT VIEW freshness_samples_mv")
    result = await ch.query("SELECT age_s FROM freshness_samples ORDER BY sampled_at DESC LIMIT 1")
    assert 0 <= result.rows[0]["age_s"] < 60


async def test_lag_counts_a_replay_of_old_events_stored_just_now(ch: ClickHouse) -> None:
    # After a two-day outage, ingest replays two-day-old events: their lag is the point.
    await ch.insert("wiki_edits", rows(10, start=datetime.now(UTC) - timedelta(days=2)))
    result = await ch.query(ops.LAG, params={"window_s": ops.LAG_WINDOW_S})
    assert result.rows[0]["events"] == 10
    assert result.rows[0]["p50_ms"] > 86_400_000


async def test_the_whole_payload_builds_from_real_tables(ch: ClickHouse) -> None:
    month = datetime.now(UTC).date().replace(day=1)
    last_month = (month - timedelta(days=1)).replace(day=1)
    await ch.insert("wiki_edits", rows(30))
    await ch.insert(
        "ingest_reconnects",
        [
            {"at": datetime.now(UTC).isoformat(), "reason": "idle"},
            {"at": datetime.now(UTC).isoformat(), "reason": "idle"},
            {"at": (datetime.now(UTC) - timedelta(days=2)).isoformat(), "reason": "eof"},
        ],
    )
    await ch.insert(
        "aws_cost",
        [
            {
                "fetched_at": (datetime.now(UTC) - timedelta(days=40)).isoformat(),
                "ok": True,
                "period_start": last_month.isoformat(),
                "period_end": month.isoformat(),
                "amount": "9.99",
                "currency": "USD",
                "estimated": False,
                "error": "",
            },
            {
                "fetched_at": datetime.now(UTC).isoformat(),
                "ok": True,
                "period_start": month.isoformat(),
                "period_end": "2026-10-09",
                "amount": "0.1977203052",
                "currency": "USD",
                "estimated": True,
                "error": "",
            },
            {
                "fetched_at": (datetime.now(UTC) + timedelta(seconds=1)).isoformat(),
                "ok": False,
                "period_start": month.isoformat(),
                "period_end": "2026-10-09",
                "amount": "",
                "currency": "",
                "estimated": False,
                "error": "AccessDenied",
            },
        ],
    )
    payload = await ops.OpsService(
        ch, ttl_s=60, threshold_s=60, target=0.99, days=30, error_cooldown_s=5
    ).build()
    assert payload["ingest"]["lag_ms"]["events"] == 30
    assert payload["ingest"]["lag_ms"]["p95"] is not None
    assert payload["ingest"]["reconnects"]["by_reason"] == {"idle": 2}
    assert payload["ingest"]["bookmark"] is None  # the test rows' ids aren't real positions
    # This month's newest successful fetch: not the failed attempt after it, and never
    # last month's total passed off as this month's.
    assert payload["cost"] is not None
    assert payload["cost"]["amount"] == "0.1977203052"
    # The latest attempt failed: the page can say the figure is from an earlier check.
    assert payload["cost_check"]["today"] == "failed"
    assert payload["ingest"]["last_stored_at"] is not None
    assert payload["ingest"]["newest_event_at"] is not None


def service(ch: ClickHouse) -> ops.OpsService:
    return ops.OpsService(ch, ttl_s=60, threshold_s=60, target=0.999, days=30, error_cooldown_s=5)


def cost_row(*, at: datetime, ok: bool, start: str) -> dict[str, object]:
    return {
        "fetched_at": at.isoformat(),
        "ok": ok,
        "period_start": start,
        "period_end": "2026-10-09",
        "amount": "1.5" if ok else "",
        "currency": "USD" if ok else "",
        "estimated": ok,
        "error": "" if ok else "AccessDenied",
    }


async def test_an_empty_database_reports_nothing_rather_than_zeros(ch: ClickHouse) -> None:
    payload = await service(ch).build()
    assert payload["ingest"]["state"] == "empty"
    assert payload["ingest"]["newest_event_at"] is None
    assert payload["ingest"]["lag_ms"]["p50"] is None
    assert payload["ingest"]["bookmark"] is None
    assert payload["cost"] is None
    assert payload["cost_check"] == {"today": "pending", "last_attempt_at": None}
    assert payload["freshness"]["ratio"] is None or payload["freshness"]["minutes"] == 0


async def test_a_first_check_that_failed_gives_no_figure(ch: ClickHouse) -> None:
    month = datetime.now(UTC).date().replace(day=1).isoformat()
    await ch.insert("aws_cost", [cost_row(at=datetime.now(UTC), ok=False, start=month)])
    payload = await service(ch).build()
    assert payload["cost"] is None
    assert payload["cost_check"]["today"] == "failed"


async def test_last_months_figure_is_never_this_months(ch: ClickHouse) -> None:
    month = datetime.now(UTC).date().replace(day=1)
    last_month = (month - timedelta(days=1)).replace(day=1)
    await ch.insert(
        "aws_cost",
        [
            cost_row(
                at=datetime.now(UTC) - timedelta(days=35), ok=True, start=last_month.isoformat()
            )
        ],
    )
    payload = await service(ch).build()
    assert payload["cost"] is None
    assert payload["cost_check"]["today"] == "pending"  # an old attempt says nothing of today


async def test_restored_rows_keep_their_own_ingest_time(ch: ClickHouse) -> None:
    # A host restored from the archive, before ingest has written anything: the rows carry
    # the time they were first stored, and no bookmark.
    restored = rows(5, start=datetime.now(UTC) - timedelta(days=1))
    for row in restored:
        row["sse_id"] = ""
        row["ingested_at"] = row["event_time"]
    await ch.insert("wiki_edits", restored)
    payload = await service(ch).build()
    assert payload["ingest"]["state"] == "paused"
    assert payload["ingest"]["bookmark"] is None
    assert payload["ingest"]["last_stored_at"] is not None
