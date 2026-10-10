"""The Ops payload (ops/report.py, api/ops.py): the bookmark, assembling, caching."""

import json
from collections.abc import Mapping
from typing import Any

import pytest

from livedemos.api import ops
from livedemos.db.clickhouse import QueryResult, QueryStats
from livedemos.ops import report, slo
from tests.stubs import StubClickHouse

REAL_ID = (
    '[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1791498391744},'
    '{"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1}]'
)


def test_the_bookmark_is_shortened_to_each_streams_position() -> None:
    bookmark = report.shorten_bookmark(REAL_ID)
    assert bookmark == {
        "positions": [
            {"stream": "eqiad", "at": "2026-10-08T22:26:31.744Z", "offset": None},
            {"stream": "codfw", "at": None, "offset": -1},
        ],
        "bytes": len(REAL_ID),
    }


def test_implausible_positions_are_left_out_not_crashed_on() -> None:
    sse_id = (
        '[{"topic":"eqiad.x","timestamp":true},{"topic":"codfw.x","timestamp":99999999999999999999},'
        '{"topic":"other.x","offset":false}]'
    )
    bookmark = report.shorten_bookmark(sse_id)
    assert bookmark is not None
    assert [(p["at"], p["offset"]) for p in bookmark["positions"]] == [(None, None)] * 3


@pytest.mark.parametrize("sse_id", ["not json", "{}", "[1]", '[{"partition":0}]'])
def test_an_unexpected_bookmark_is_left_out_rather_than_guessed(sse_id: str) -> None:
    assert report.shorten_bookmark(sse_id) is None


POLICY = report.Policy(threshold_s=60.0, target=0.99, days=30, stale_after_s=60.0)


def empty_assemble(now: float = 1_791_500_000.0, **rows: Any) -> Any:
    lag = rows.pop("lag", {"events": 0, "p50_ms": 0, "p95_ms": 0})
    return report.assemble(now, report.Rows(lag=lag, **rows), POLICY)


def test_nothing_measured_yet_is_null_not_zero() -> None:
    payload = empty_assemble()
    # quantileExact over no rows says 0; with no events that means "don't know".
    assert payload["ingest"]["lag_ms"]["p50"] is None
    assert payload["ingest"]["bookmark"] is None
    assert payload["freshness"]["ratio"] is None
    assert payload["freshness"]["from"] is None
    assert payload["cost"] is None
    assert payload["cost_check"] == {"today": "pending", "last_attempt_at": None}
    assert payload["ingest"]["state"] == "empty"
    assert payload["ingest"]["newest_event_at"] is None
    assert payload["ingest"]["last_stored_at"] is None


NOW_S = 1_791_500_000.0  # 2026-10-08T22:53:20Z


def test_paused_ingest_and_a_failed_cost_check_are_reported_with_times() -> None:
    payload = empty_assemble(
        now=NOW_S,
        head={"newest_ms": 1_791_499_000_000, "stored_ms": 1_791_499_000_400, "n": 5},
        cost_check={"fetched_ms": 1_791_499_900_000, "ok": 0},
    )
    assert payload["ingest"]["state"] == "paused"
    assert payload["ingest"]["newest_event_at"] == "2026-10-08T22:36:40.000Z"
    assert payload["ingest"]["last_stored_at"] == "2026-10-08T22:36:40.400Z"
    assert payload["ingest"]["stale_after_s"] == 60.0
    assert payload["cost_check"] == {
        "today": "failed",
        "last_attempt_at": "2026-10-08T22:51:40.000Z",
    }  # never the error text: it can hold an account id


@pytest.mark.parametrize(
    ("newest_age_s", "stored_age_s", "state"),
    [
        (2, 1, "live"),
        (60, 1, "live"),  # the limit itself is still live, as in the widget
        (172_800, 1, "catching_up"),  # replaying two-day-old events, stored just now
        (172_800, 61, "paused"),
        (61, 61, "paused"),
    ],
)
def test_ingest_state(newest_age_s: float, stored_age_s: float, state: str) -> None:
    got = report.ingest_state(
        now=NOW_S, newest_s=NOW_S - newest_age_s, stored_s=NOW_S - stored_age_s, stale_after_s=60
    )
    assert got == state


@pytest.mark.parametrize(
    ("latest", "today"),
    [
        (None, "pending"),  # never attempted
        ({"fetched_ms": 1_791_499_900_000, "ok": 1}, "ok"),
        ({"fetched_ms": 1_791_499_900_000, "ok": 0}, "failed"),
        # Yesterday's attempt, nothing today: not asked yet, or its answer was lost.
        ({"fetched_ms": 1_791_400_000_000, "ok": 1}, "pending"),
        ({"fetched_ms": 1_791_400_000_000, "ok": 0}, "pending"),
    ],
)
def test_cost_today_judges_only_todays_attempt(latest: dict[str, int] | None, today: str) -> None:
    assert report.cost_today(now=NOW_S, latest=latest)["today"] == today


def test_assemble_reports_every_number_it_was_given() -> None:
    win = slo.Window(start_s=1_791_496_400, end_s=1_791_500_000)
    payload = empty_assemble(
        lag={"events": 120, "p50_ms": 900, "p95_ms": 2_400},
        sse_id=REAL_ID,
        reconnects=[{"reason": "idle", "n": 3}, {"reason": "eof", "n": 1}],
        freshness=slo.summarise(win=win, sampled=58, fresh=57, target=0.99, days=30),
        gaps=[{"from_ms": 1_000_000, "to_ms": 1_090_500, "reason": "retention", "total": 21}],
        cost={
            "fetched_ms": 1_791_498_000_000,
            "start_day": "2026-10-01",
            "end_day": "2026-10-09",
            "amount": "0.1977203052",
            "currency": "USD",
            "estimated": 1,
        },
    )
    assert payload["ingest"]["lag_ms"] == {
        "p50": 900,
        "p95": 2_400,
        "events": 120,
        "window_s": 3_600,
    }
    assert payload["ingest"]["reconnects"]["total"] == 4
    assert payload["ingest"]["reconnects"]["by_reason"] == {"idle": 3, "eof": 1}
    fresh = payload["freshness"]
    assert (fresh["minutes"], fresh["fresh"], fresh["stale"], fresh["unmeasured"]) == (60, 57, 1, 2)
    assert fresh["ratio"] == 0.95
    assert payload["gaps"]["recent"][0]["duration_s"] == 90
    assert payload["gaps"]["total"] == 21  # more than the list holds: the page can say so
    assert fresh["full_window"] is False
    assert payload["cost"]["amount"] == "0.1977203052"
    assert payload["cost"]["currency"] == "USD"
    assert payload["cost"]["fetched_at"] == "2026-10-08T22:20:00.000Z"


class StubDatabase(StubClickHouse):
    def answer(self, sql: str, params: Mapping[str, Any]) -> QueryResult:
        rows: list[dict[str, Any]] = []
        if sql is report.FIRST_SAMPLE:
            rows = [{"first_ms": 0, "n": 0}]
        elif sql is report.LAG:
            rows = [{"events": 0, "p50_ms": 0, "p95_ms": 0}]
        return QueryResult(rows, QueryStats(0.0, 0, 0))


def service(db: StubDatabase, *, ttl_s: float = 60, cooldown_s: float = 60) -> ops.OpsService:
    return ops.OpsService(db, policy=POLICY, ttl_s=ttl_s, error_cooldown_s=cooldown_s)


async def test_one_build_serves_every_request_until_it_expires() -> None:
    db = StubDatabase()
    svc = service(db)
    first, age = await svc.get()
    assert age == 0
    assert json.loads(first)["freshness"]["minutes"] == 0
    built = len(db.queries)
    again, age = await svc.get()
    assert again == first
    assert 0 <= age < 60  # tells the route how much of the minute is left for the CDN
    assert len(db.queries) == built


async def test_a_failure_is_remembered_for_the_cooldown() -> None:
    db = StubDatabase()
    db.fail_queries = True
    svc = service(db)
    with pytest.raises(ops.OpsUnavailable):
        await svc.get()
    tried = len(db.queries)
    with pytest.raises(ops.OpsUnavailable):
        await svc.get()
    assert len(db.queries) == tried


def test_the_figure_and_the_check_come_from_the_same_read() -> None:
    row = {
        "attempts": 400,  # however many repeated rows: aggregates, so nothing is pushed out
        "fetched_ms": 1_791_499_900_000,
        "last_ok": 0,
        "figures": 3,
        "figure_ms": 1_791_400_000_000,
        "fig_start": "2026-10-01",
        "fig_end": "2026-10-08",
        "fig_amount": "1.2",
        "fig_currency": "USD",
        "fig_estimated": 1,
    }
    figure, latest = report.split_cost(row)
    assert figure is not None
    assert figure["amount"] == "1.2"  # this month's newest success, not the failure after it
    assert latest == {"fetched_ms": 1_791_499_900_000, "ok": 0}
    assert report.split_cost({**row, "figures": 0}) == (None, latest)  # no figure this month
    assert report.split_cost({**row, "attempts": 0}) == (None, None)
    assert report.month_start(NOW_S) == "2026-10-01"
