"""GET /v1/ops.json: how the pipeline itself is doing, for the Ops tab.

Every number on the tab comes from here, and every number here from ClickHouse:

- ingest lag, p50 and p95, over what ingest committed in the last hour,
- the bookmark ingest would resume from, shortened to each stream's position,
- stream reconnects in the last 24 hours, by reason (written by ingest),
- the 30-day freshness SLO (ops/slo.py, from samples ClickHouse takes every minute),
- gaps recorded in the last 30 days,
- month-to-date AWS cost, as Cost Explorer last reported it (ops/cost.py).

The answer is built at most once a minute and shared by every viewer; CloudFront caches it
for the same minute in front.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping, Sequence
from typing import Any

from livedemos.api.contract import (
    Bookmark,
    BookmarkPosition,
    CostReport,
    FreshnessReport,
    GapReport,
    OpsPayload,
    iso,
)
from livedemos.clickhouse import ClickHouseError, Queryable
from livedemos.ops import slo

LAG_WINDOW_S = 3_600
RECONNECT_WINDOW_S = 86_400
GAP_WINDOW_DAYS = 30
MAX_GAPS = 20

# What ingest committed in the last hour, late or replayed events included: their lag is
# real. The event-time bound only lets ClickHouse skip old partitions; a replay of events
# more than a day old is left out.
LAG = """
SELECT
    count() AS events,
    quantileExact(0.5)(dateDiff('millisecond', event_time, ingested_at)) AS p50_ms,
    quantileExact(0.95)(dateDiff('millisecond', event_time, ingested_at)) AS p95_ms
FROM wiki_edits
WHERE event_time > now() - INTERVAL 1 DAY
  AND ingested_at > now() - toIntervalSecond({window_s:UInt32})
"""

# The newest committed row's bookmark: max(ingest_seq) from the seq_max projection, then a
# key range on by_ingest_seq (migration 0002). Rows restored from the archive have none.
BOOKMARK = """
SELECT sse_id
FROM wiki_edits
WHERE ingest_seq >= (SELECT max(ingest_seq) FROM wiki_edits) AND sse_id != ''
ORDER BY ingest_seq DESC
LIMIT 1
"""

RECONNECTS = """
SELECT reason, count() AS n
FROM ingest_reconnects
WHERE at > now() - toIntervalSecond({window_s:UInt32})
GROUP BY reason
ORDER BY n DESC, reason
"""

FIRST_SAMPLE = "SELECT toUnixTimestamp(min(minute)) AS first_s, count() AS n FROM freshness_samples"

# A minute with two samples (a refresh retried) is fresh only if both were.
FRESHNESS = """
SELECT count() AS sampled, countIf(fresh) AS fresh
FROM
(
    SELECT minute, min(ifNull(age_s, inf) < {threshold_s:Float64}) AS fresh
    FROM freshness_samples
    WHERE minute >= fromUnixTimestamp({start_s:UInt32})
      AND minute < fromUnixTimestamp({end_s:UInt32})
    GROUP BY minute
)
"""

GAPS = """
SELECT
    toUnixTimestamp64Milli(gap_from) AS from_ms,
    toUnixTimestamp64Milli(gap_to) AS to_ms,
    reason
FROM ingest_gaps
WHERE gap_to > now() - toIntervalDay({days:UInt16})
ORDER BY gap_from DESC
LIMIT {limit:UInt16}
"""

COST = """
SELECT
    toUnixTimestamp64Milli(fetched_at) AS fetched_ms,
    toString(period_start) AS period_start,
    toString(period_end) AS period_end,
    amount, currency, estimated
FROM aws_cost
WHERE ok
ORDER BY fetched_at DESC
LIMIT 1
"""


def shorten_bookmark(sse_id: str) -> Bookmark | None:
    """Each stream's position from an EventStreams id, without the noise.

    The id is a JSON list of Kafka positions, one per data centre's topic, like
    `[{"topic": "eqiad.mediawiki.recentchange", "partition": 0, "timestamp": 1791...}]`.
    A topic not seen yet has an offset (-1) instead of a timestamp.
    """
    try:
        parts = json.loads(sse_id)
    except ValueError:
        return None
    if not isinstance(parts, list):
        return None
    positions: list[BookmarkPosition] = []
    for part in parts:
        if not isinstance(part, dict) or "topic" not in part:
            return None
        stream = str(part["topic"]).split(".", 1)[0]
        ts = part.get("timestamp")
        positions.append(
            {
                "stream": stream,
                "at": iso(int(ts) / 1000) if isinstance(ts, int) else None,
                "offset": part.get("offset") if isinstance(part.get("offset"), int) else None,
            }
        )
    return {"positions": positions, "bytes": len(sse_id.encode())}


def assemble(
    *,
    now: float,
    lag: Mapping[str, Any] | None,
    sse_id: str | None,
    reconnects: Sequence[Mapping[str, Any]],
    freshness: slo.Freshness | None,
    threshold_s: float,
    target: float,
    days: int,
    gaps: Sequence[Mapping[str, Any]],
    cost: Mapping[str, Any] | None,
) -> OpsPayload:
    """Pure function: query rows in, payload out."""
    events = int(lag["events"]) if lag else 0
    by_reason = {str(r["reason"]): int(r["n"]) for r in reconnects}

    fresh_report: FreshnessReport = {
        "target": target,
        "threshold_s": threshold_s,
        "window_days": days,
        "from": None,
        "to": None,
        "minutes": 0,
        "fresh": 0,
        "stale": 0,
        "unmeasured": 0,
        "ratio": None,
        "met": None,
        "budget_minutes": round(days * 1_440 * (1 - target)),
        "budget_used": 0,
    }
    if freshness is not None:
        fresh_report.update(
            {
                "from": iso(freshness.window.start_s),
                "to": iso(freshness.window.end_s),
                "minutes": freshness.window.minutes,
                "fresh": freshness.fresh,
                "stale": freshness.stale,
                "unmeasured": freshness.unmeasured,
                "ratio": None if freshness.ratio is None else round(freshness.ratio, 6),
                "met": freshness.met,
                "budget_minutes": freshness.budget_minutes,
                "budget_used": freshness.budget_used,
            }
        )

    gap_reports: list[GapReport] = [
        {
            "from": iso(int(g["from_ms"]) / 1000),
            "to": iso(int(g["to_ms"]) / 1000),
            "duration_s": max(0, round((int(g["to_ms"]) - int(g["from_ms"])) / 1000)),
            "reason": str(g["reason"]),
        }
        for g in gaps
    ]

    cost_report: CostReport | None = None
    if cost:
        cost_report = {
            "amount": str(cost["amount"]),
            "currency": str(cost["currency"]),
            "estimated": bool(cost["estimated"]),
            "period_start": str(cost["period_start"]),
            "period_end": str(cost["period_end"]),
            "fetched_at": iso(int(cost["fetched_ms"]) / 1000),
            "source": "AWS Cost Explorer, UnblendedCost, tag project=livedemos",
        }

    return {
        "generated_at": iso(now),
        "ingest": {
            "lag_ms": {
                "p50": int(lag["p50_ms"]) if events and lag else None,
                "p95": int(lag["p95_ms"]) if events and lag else None,
                "events": events,
                "window_s": LAG_WINDOW_S,
            },
            "bookmark": shorten_bookmark(sse_id) if sse_id else None,
            "reconnects": {
                "window_s": RECONNECT_WINDOW_S,
                "total": sum(by_reason.values()),
                "by_reason": by_reason,
            },
        },
        "freshness": fresh_report,
        "gaps": {"window_days": GAP_WINDOW_DAYS, "recent": gap_reports},
        "cost": cost_report,
    }


class OpsUnavailable(RuntimeError):
    pass


class OpsService:
    """Builds the payload at most once per `ttl_s`; concurrent requests share one build."""

    def __init__(
        self,
        db: Queryable,
        *,
        ttl_s: float,
        threshold_s: float,
        target: float,
        days: int,
        error_cooldown_s: float,
    ):
        self._db = db
        self._ttl_s = ttl_s
        self._threshold_s = threshold_s
        self._target = target
        self._days = days
        self._error_cooldown_s = error_cooldown_s
        self._lock = asyncio.Lock()
        self._cached: tuple[float, bytes] | None = None
        self._failed_at: float | None = None

    async def get(self) -> bytes:
        async with self._lock:
            now = time.monotonic()
            if self._cached and now - self._cached[0] < self._ttl_s:
                return self._cached[1]
            if self._failed_at is not None and now - self._failed_at < self._error_cooldown_s:
                raise OpsUnavailable("ops data unavailable")
            try:
                payload = await self.build()
            except ClickHouseError as exc:
                self._failed_at = time.monotonic()
                raise OpsUnavailable("ops data unavailable") from exc
            body = json.dumps(payload, separators=(",", ":")).encode()
            self._cached, self._failed_at = (time.monotonic(), body), None
            return body

    async def build(self) -> OpsPayload:
        now = time.time()
        lag, bookmark, reconnects, first, gaps, cost = await asyncio.gather(
            self._db.query(LAG, params={"window_s": LAG_WINDOW_S}),
            self._db.query(BOOKMARK),
            self._db.query(RECONNECTS, params={"window_s": RECONNECT_WINDOW_S}),
            self._db.query(FIRST_SAMPLE),
            self._db.query(GAPS, params={"days": GAP_WINDOW_DAYS, "limit": MAX_GAPS}),
            self._db.query(COST),
        )
        first_s = int(first.rows[0]["first_s"]) if first.rows and int(first.rows[0]["n"]) else None
        win = slo.window(now_s=now, first_sample_s=first_s, days=self._days)
        freshness = None
        if win is not None:
            counts = await self._db.query(
                FRESHNESS,
                params={
                    "threshold_s": self._threshold_s,
                    "start_s": win.start_s,
                    "end_s": win.end_s,
                },
            )
            row = counts.rows[0] if counts.rows else {"sampled": 0, "fresh": 0}
            freshness = slo.summarise(
                win=win,
                sampled=int(row["sampled"]),
                fresh=int(row["fresh"]),
                target=self._target,
                days=self._days,
            )
        return assemble(
            now=now,
            lag=lag.rows[0] if lag.rows else None,
            sse_id=str(bookmark.rows[0]["sse_id"]) if bookmark.rows else None,
            reconnects=reconnects.rows,
            freshness=freshness,
            threshold_s=self._threshold_s,
            target=self._target,
            days=self._days,
            gaps=gaps.rows,
            cost=cost.rows[0] if cost.rows else None,
        )
