"""The Ops payload (api/ops.py): shortening the bookmark, assembling, caching."""

import json
from collections.abc import Mapping
from typing import Any

import pytest

from livedemos.api import ops
from livedemos.clickhouse import ClickHouseError, QueryResult, QueryStats
from livedemos.ops import slo

REAL_ID = (
    '[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1791498391744},'
    '{"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1}]'
)


def test_the_bookmark_is_shortened_to_each_streams_position() -> None:
    bookmark = ops.shorten_bookmark(REAL_ID)
    assert bookmark == {
        "positions": [
            {"stream": "eqiad", "at": "2026-10-08T22:26:31.744Z", "offset": None},
            {"stream": "codfw", "at": None, "offset": -1},
        ],
        "bytes": len(REAL_ID),
    }


@pytest.mark.parametrize("sse_id", ["not json", "{}", "[1]", '[{"partition":0}]'])
def test_an_unexpected_bookmark_is_left_out_rather_than_guessed(sse_id: str) -> None:
    assert ops.shorten_bookmark(sse_id) is None


def empty_assemble(**overrides: Any) -> Any:
    args: dict[str, Any] = {
        "now": 1_791_500_000.0,
        "lag": {"events": 0, "p50_ms": 0, "p95_ms": 0},
        "sse_id": None,
        "reconnects": [],
        "freshness": None,
        "threshold_s": 60.0,
        "target": 0.99,
        "days": 30,
        "gaps": [],
        "cost": None,
    }
    return ops.assemble(**{**args, **overrides})


def test_nothing_measured_yet_is_null_not_zero() -> None:
    payload = empty_assemble()
    # quantileExact over no rows says 0; with no events that means "don't know".
    assert payload["ingest"]["lag_ms"]["p50"] is None
    assert payload["ingest"]["bookmark"] is None
    assert payload["freshness"]["ratio"] is None
    assert payload["freshness"]["from"] is None
    assert payload["cost"] is None


def test_assemble_reports_every_number_it_was_given() -> None:
    win = slo.Window(start_s=1_791_496_400, end_s=1_791_500_000)
    payload = empty_assemble(
        lag={"events": 120, "p50_ms": 900, "p95_ms": 2_400},
        sse_id=REAL_ID,
        reconnects=[{"reason": "idle", "n": 3}, {"reason": "eof", "n": 1}],
        freshness=slo.summarise(win=win, sampled=58, fresh=57, target=0.99, days=30),
        gaps=[{"from_ms": 1_000_000, "to_ms": 1_090_500, "reason": "retention"}],
        cost={
            "fetched_ms": 1_791_498_000_000,
            "period_start": "2026-10-01",
            "period_end": "2026-10-09",
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
    assert payload["cost"]["amount"] == "0.1977203052"
    assert payload["cost"]["currency"] == "USD"
    assert payload["cost"]["fetched_at"] == "2026-10-08T22:20:00.000Z"


class StubDatabase:
    def __init__(self) -> None:
        self.queries = 0
        self.fail = False

    async def query(
        self, sql: str, *, params: Mapping[str, Any] | None = None, **_: Any
    ) -> QueryResult:
        self.queries += 1
        if self.fail:
            raise ClickHouseError("down")
        rows: list[dict[str, Any]] = []
        if sql is ops.FIRST_SAMPLE:
            rows = [{"first_s": 0, "n": 0}]
        elif sql is ops.LAG:
            rows = [{"events": 0, "p50_ms": 0, "p95_ms": 0}]
        return QueryResult(rows, QueryStats(0.0, 0, 0))


def service(db: StubDatabase, *, ttl_s: float = 60, cooldown_s: float = 60) -> ops.OpsService:
    return ops.OpsService(
        db, ttl_s=ttl_s, threshold_s=60, target=0.99, days=30, error_cooldown_s=cooldown_s
    )


async def test_one_build_serves_every_request_until_it_expires() -> None:
    db = StubDatabase()
    svc = service(db)
    first = await svc.get()
    assert json.loads(first)["freshness"]["minutes"] == 0
    built = db.queries
    assert await svc.get() == first
    assert db.queries == built


async def test_a_failure_is_remembered_for_the_cooldown() -> None:
    db = StubDatabase()
    db.fail = True
    svc = service(db)
    with pytest.raises(ops.OpsUnavailable):
        await svc.get()
    tried = db.queries
    with pytest.raises(ops.OpsUnavailable):
        await svc.get()
    assert db.queries == tried
