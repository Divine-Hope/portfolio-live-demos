"""The Ops tab's numbers (GET /v1/ops.json): the queries, and the payload built from them.

Every number comes from ClickHouse. The payload's shape is `api/contract.py`'s OpsPayload.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
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
)
from livedemos.dates import iso
from livedemos.db.clickhouse import Queryable
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

# The latest attempt (failed or not) and this month's latest figure, in one read so they
# can't disagree, and by aggregate so no number of repeated rows can push either out. Only
# `ok` and the time of an attempt go out, never the error text: it can hold an account id,
# and the page is public.
COST = """
SELECT
    count() AS attempts,
    toUnixTimestamp64Milli(max(fetched_at)) AS fetched_ms,
    argMax(ok, fetched_at) AS last_ok,
    countIf(ok AND period_start = {month:Date}) AS figures,
    toUnixTimestamp64Milli(maxIf(fetched_at, ok AND period_start = {month:Date})) AS figure_ms,
    toString(argMaxIf(period_start, fetched_at, ok AND period_start = {month:Date})) AS fig_start,
    toString(argMaxIf(period_end, fetched_at, ok AND period_start = {month:Date})) AS fig_end,
    argMaxIf(amount, fetched_at, ok AND period_start = {month:Date}) AS fig_amount,
    argMaxIf(currency, fetched_at, ok AND period_start = {month:Date}) AS fig_currency,
    argMaxIf(estimated, fetched_at, ok AND period_start = {month:Date}) AS fig_estimated
FROM aws_cost
"""


def month_start(now: float) -> str:
    """The first day of the current UTC month: only its figures are this month's."""
    return datetime.fromtimestamp(now, UTC).date().replace(day=1).isoformat()


def split_cost(
    row: Mapping[str, Any] | None,
) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
    """(this month's figure, the latest attempt) from the COST row; None for either missing."""
    if not row or not int(row["attempts"]):
        return None, None
    latest = {"fetched_ms": row["fetched_ms"], "ok": row["last_ok"]}
    if not int(row["figures"]):
        return None, latest
    figure = {
        "fetched_ms": row["figure_ms"],
        "start_day": row["fig_start"],
        "end_day": row["fig_end"],
        "amount": row["fig_amount"],
        "currency": row["fig_currency"],
        "estimated": row["fig_estimated"],
    }
    return figure, latest


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


@dataclass(frozen=True, slots=True)
class Policy:
    """What the report is judged against."""

    threshold_s: float  # freshness: the newest event younger than this
    target: float  # the SLO: this share of minutes fresh
    days: int  # over this many days
    stale_after_s: float  # older newest event: ingest isn't live


@dataclass(frozen=True, slots=True)
class Rows:
    """What the queries returned, already reduced to what assemble() needs."""

    lag: Mapping[str, Any] | None = None
    sse_id: str | None = None
    reconnects: Sequence[Mapping[str, Any]] = ()
    freshness: slo.Freshness | None = None
    gaps: Sequence[Mapping[str, Any]] = ()
    cost: Mapping[str, Any] | None = None  # this month's figure
    cost_check: Mapping[str, Any] | None = None  # the latest attempt
    head: Mapping[str, Any] | None = None


async def collect(db: Queryable, *, now: float, policy: Policy) -> Rows:
    lag, bookmark, reconnects, first, gaps, cost, head = await asyncio.gather(
        db.query(LAG, params={"window_s": LAG_WINDOW_S}),
        db.query(BOOKMARK),
        db.query(RECONNECTS, params={"window_s": RECONNECT_WINDOW_S}),
        db.query(FIRST_SAMPLE),
        db.query(GAPS, params={"days": GAP_WINDOW_DAYS, "limit": MAX_GAPS}),
        db.query(COST, params={"month": month_start(now)}),
        db.query(HEAD),
    )
    first_ms = int(first.rows[0]["first_ms"]) if first.rows and int(first.rows[0]["n"]) else None
    win = slo.window(
        now_s=now, first_sample_s=None if first_ms is None else first_ms / 1000, days=policy.days
    )
    freshness = None
    if win is not None:
        counts = await db.query(
            FRESHNESS,
            params={"threshold_s": policy.threshold_s, "start_s": win.start_s, "end_s": win.end_s},
        )
        row = counts.rows[0] if counts.rows else {"sampled": 0, "fresh": 0}
        freshness = slo.summarise(
            win=win,
            sampled=int(row["sampled"]),
            fresh=int(row["fresh"]),
            target=policy.target,
            days=policy.days,
        )
    figure, latest = split_cost(cost.rows[0] if cost.rows else None)
    return Rows(
        lag=lag.rows[0] if lag.rows else None,
        sse_id=str(bookmark.rows[0]["sse_id"]) if bookmark.rows else None,
        reconnects=reconnects.rows,
        freshness=freshness,
        gaps=gaps.rows,
        cost=figure,
        cost_check=latest,
        head=head.rows[0] if head.rows else None,
    )


def assemble(now: float, rows: Rows, policy: Policy) -> OpsPayload:
    """Pure function: query rows in, payload out."""
    lag, reconnects, freshness, gaps, cost, head = (
        rows.lag,
        rows.reconnects,
        rows.freshness,
        rows.gaps,
        rows.cost,
        rows.head,
    )
    days, target, threshold_s = policy.days, policy.target, policy.threshold_s
    stale_after_s = policy.stale_after_s
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
        "budget_minutes": slo.budget_minutes(days=days, target=target),
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
            "bookmark": shorten_bookmark(rows.sse_id) if rows.sse_id else None,
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
        "cost_check": cost_today(now=now, latest=rows.cost_check),
    }
