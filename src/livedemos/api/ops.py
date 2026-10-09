"""GET /v1/ops.json: how the pipeline itself is doing, for the Ops tab.

Every number on the tab comes from here, and every number here from ClickHouse:

- what ingest is doing (live, catching up, paused, or no data yet), with when the newest
  event happened and when ingest last stored a row, so the page can say "paused since"
  with a real time,
- ingest lag, p50 and p95, over what ingest committed in the last hour,
- the bookmark ingest would resume from, shortened to each stream's position,
- stream reconnects in the last 24 hours, by reason (written by ingest),
- the 30-day freshness SLO (ops/slo.py, from samples ClickHouse takes every minute),
- gaps recorded in the last 30 days,
- month-to-date AWS cost, as Cost Explorer last reported it (ops/cost.py), and how
  today's daily check went, so the page can say a figure is from an earlier day.

The answer is built at most once a minute and shared by every viewer. CloudFront caches it
for what's left of that minute, so no copy is more than a minute old.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, TypeGuard

from livedemos.api.contract import (
    Bookmark,
    BookmarkPosition,
    CostCheck,
    CostReport,
    FreshnessReport,
    GapReport,
    IngestState,
    OpsPayload,
    iso,
)
from livedemos.clickhouse import ClickHouseError, Queryable
from livedemos.ops import slo

LAG_WINDOW_S = 3_600
RECONNECT_WINDOW_S = 86_400
GAP_WINDOW_DAYS = 30
MAX_GAPS = 20

# Everything ingest stored in the last hour, late and replayed events included: their lag
# is real, and a catch-up after an outage is when it matters most. The minmax index on
# ingested_at (migration 0005) skips the granules outside the hour.
LAG = """
SELECT
    count() AS events,
    quantileExact(0.5)(dateDiff('millisecond', event_time, ingested_at)) AS p50_ms,
    quantileExact(0.95)(dateDiff('millisecond', event_time, ingested_at)) AS p95_ms
FROM wiki_edits
WHERE ingested_at > now() - toIntervalSecond({window_s:UInt32})
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

FIRST_SAMPLE = (
    "SELECT toUnixTimestamp64Milli(min(sampled_at)) AS first_ms, count() AS n "
    "FROM freshness_samples"
)

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
    reason,
    count() OVER () AS total
FROM ingest_gaps
WHERE gap_to > now() - toIntervalDay({days:UInt16})
ORDER BY gap_from DESC
LIMIT {limit:UInt16}
"""

# max(event_time) comes from part metadata; max(ingested_at) reads one column of the week.
HEAD = """
SELECT
    toUnixTimestamp64Milli(max(event_time)) AS newest_ms,
    toUnixTimestamp64Milli(max(ingested_at)) AS stored_ms,
    count() AS n
FROM wiki_edits
"""

# The latest daily attempt, failed or not. Only whether it worked goes out: the error text
# can hold an account id, and the repo and page are public.
COST_CHECK = """
SELECT toUnixTimestamp64Milli(fetched_at) AS fetched_ms, ok
FROM aws_cost
ORDER BY fetched_at DESC
LIMIT 1
"""

COST = """
SELECT
    toUnixTimestamp64Milli(fetched_at) AS fetched_ms,
    toString(period_start) AS start_day,
    toString(period_end) AS end_day,
    amount, currency, estimated
FROM aws_cost
WHERE ok AND period_start = toStartOfMonth(toDate(now(), 'UTC'))  -- this month's, only
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
        ts, offset = part.get("timestamp"), part.get("offset")
        positions.append(
            {
                "stream": str(part["topic"]).split(".", 1)[0],
                "at": iso(ts / 1000) if _plausible_ms(ts) else None,
                "offset": offset if _is_int(offset) else None,
            }
        )
    return {"positions": positions, "bytes": len(sse_id.encode())}


def _is_int(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _plausible_ms(value: object) -> TypeGuard[int]:
    """A Unix time in ms between 2000 and 2100: anything else isn't a position to show."""
    if not isinstance(value, int) or isinstance(value, bool):
        return False
    return 946_684_800_000 <= value < 4_102_444_800_000


def ingest_state(
    *, now: float, newest_s: float | None, stored_s: float | None, stale_after_s: float
) -> IngestState:
    """What ingest is doing, judged here so every viewer of the page agrees.

    - live: the newest event is recent.
    - catching_up: the newest event is old, but rows are still being stored (replaying an
      outage from the stream). Not paused: it's working through the backlog.
    - paused: nothing stored recently, and the newest event is old.
    - empty: no rows at all.
    """
    if newest_s is None or stored_s is None:
        return "empty"
    if now - newest_s <= stale_after_s:
        return "live"
    if now - stored_s <= stale_after_s:
        return "catching_up"
    return "paused"


def cost_today(*, now: float, latest: Mapping[str, Any] | None) -> CostCheck:
    """How today's (UTC) Cost Explorer check went, from the latest attempt on record.

    - ok, failed: today's attempt is recorded, and worked or didn't.
    - pending: nothing recorded today. Not asked yet, or asked and its answer lost (no
      second call is made that day). Either way the figure, if any, is from an earlier day.
    """
    today = datetime.fromtimestamp(now, UTC).date()
    if latest is None:
        return {"today": "pending", "last_attempt_at": None}
    at = int(latest["fetched_ms"]) / 1000
    if datetime.fromtimestamp(at, UTC).date() != today:
        return {"today": "pending", "last_attempt_at": iso(at)}
    return {"today": "ok" if bool(latest["ok"]) else "failed", "last_attempt_at": iso(at)}


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
    head: Mapping[str, Any] | None = None,
    cost_check: Mapping[str, Any] | None = None,
    stale_after_s: float = 60.0,
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
        "full_window": False,
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
                "full_window": freshness.window.is_full(days),
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
            "period_start": str(cost["start_day"]),
            "period_end": str(cost["end_day"]),
            "fetched_at": iso(int(cost["fetched_ms"]) / 1000),
            "source": "AWS Cost Explorer, UnblendedCost, tag project=livedemos",
        }

    has_rows = bool(head and int(head["n"]))
    newest_s = int(head["newest_ms"]) / 1000 if head and has_rows else None
    stored_s = int(head["stored_ms"]) / 1000 if head and has_rows else None
    return {
        "generated_at": iso(now),
        "ingest": {
            "state": ingest_state(
                now=now, newest_s=newest_s, stored_s=stored_s, stale_after_s=stale_after_s
            ),
            "newest_event_at": None if newest_s is None else iso(newest_s),
            "last_stored_at": None if stored_s is None else iso(stored_s),
            "stale_after_s": stale_after_s,
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
        "gaps": {
            "window_days": GAP_WINDOW_DAYS,
            "total": int(gaps[0]["total"]) if gaps else 0,  # `recent` holds the newest 20
            "recent": gap_reports,
        },
        "cost": cost_report,
        "cost_check": cost_today(now=now, latest=cost_check),
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
        stale_after_s: float = 60.0,
    ):
        self._db = db
        self._stale_after_s = stale_after_s
        self._ttl_s = ttl_s
        self._threshold_s = threshold_s
        self._target = target
        self._days = days
        self._error_cooldown_s = error_cooldown_s
        self._lock = asyncio.Lock()
        self._cached: tuple[float, bytes] | None = None
        self._failed_at: float | None = None

    async def get(self) -> tuple[bytes, float]:
        """The payload, and how many seconds ago it was built."""
        async with self._lock:
            now = time.monotonic()
            if self._cached and now - self._cached[0] < self._ttl_s:
                return self._cached[1], now - self._cached[0]
            if self._failed_at is not None and now - self._failed_at < self._error_cooldown_s:
                raise OpsUnavailable("ops data unavailable")
            try:
                payload = await self.build()
            except ClickHouseError as exc:
                self._failed_at = time.monotonic()
                raise OpsUnavailable("ops data unavailable") from exc
            body = json.dumps(payload, separators=(",", ":")).encode()
            self._cached, self._failed_at = (time.monotonic(), body), None
            return body, 0.0

    async def build(self) -> OpsPayload:
        now = time.time()
        lag, bookmark, reconnects, first, gaps, cost, head, check = await asyncio.gather(
            self._db.query(LAG, params={"window_s": LAG_WINDOW_S}),
            self._db.query(BOOKMARK),
            self._db.query(RECONNECTS, params={"window_s": RECONNECT_WINDOW_S}),
            self._db.query(FIRST_SAMPLE),
            self._db.query(GAPS, params={"days": GAP_WINDOW_DAYS, "limit": MAX_GAPS}),
            self._db.query(COST),
            self._db.query(HEAD),
            self._db.query(COST_CHECK),
        )
        first_ms = (
            int(first.rows[0]["first_ms"]) if first.rows and int(first.rows[0]["n"]) else None
        )
        win = slo.window(
            now_s=now, first_sample_s=None if first_ms is None else first_ms / 1000, days=self._days
        )
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
            head=head.rows[0] if head.rows else None,
            cost_check=check.rows[0] if check.rows else None,
            stale_after_s=self._stale_after_s,
        )
