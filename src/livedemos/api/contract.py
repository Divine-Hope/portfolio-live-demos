"""The JSON the API returns: its shapes, and the formatting every endpoint shares.

The widget is the main consumer (web/embed/wikipedia/widget.js). Policy it needs, such as
when data counts as stale, travels in the payload instead of being hardcoded twice.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal, TypedDict
from urllib.parse import quote


def iso(ts: float) -> str:
    """Unix seconds as ISO 8601 UTC with milliseconds: `2026-10-04T18:00:00.123Z`."""
    return datetime.fromtimestamp(ts, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def article_url(lang: str, title: str) -> str:
    return f"https://{lang}.wikipedia.org/wiki/{quote(title.replace(' ', '_'), safe='')}"


class Article(TypedDict):
    title: str
    lang: str
    edits: int
    url: str


class MinuteBucket(TypedDict):
    t: int  # minute start, unix seconds
    edits: int | None  # None: unknown (no data, or inside a recorded gap), never a fake zero
    partial: bool  # the newest minute, still filling


class LangSummary(TypedDict):
    edits_5m: int
    pages_5m: int
    bot_share_5m: float | None
    top_articles: list[Article]
    per_minute: list[MinuteBucket]


class IngestLag(TypedDict):
    p50: int | None
    p95: int | None


class _LiveBase(TypedDict):
    dataset: Literal["wikipedia"]
    status: Literal["empty", "live", "stale", "fallback"]  # "fallback": S3 copy, host down
    computed_at: str
    as_of: str | None
    last_event_age_s: float | None
    stale_after_s: float
    langs: dict[str, LangSummary]


class LivePayload(_LiveBase, total=False):
    ingest_lag_ms: IngestLag  # absent while empty
    window_s: int


class QueryInfo(TypedDict):
    elapsed_ms: float
    rows_read: int
    bytes_read: int
    cache: Literal["hit", "miss"]


class ActivityPayload(TypedDict):
    lang: str
    window: str
    generated_at: str
    edits: int
    pages_edited: int | None  # None only when a 3d/7d window starts past the 14 days kept
    bot_share: float | None
    as_of: str | None
    last_event_age_s: float | None
    query: QueryInfo


class LagReport(TypedDict):
    p50: int | None  # None when nothing was committed in the window
    p95: int | None
    events: int
    window_s: int


class BookmarkPosition(TypedDict):
    stream: str  # the data centre's topic prefix: eqiad, codfw
    at: str | None  # the position, as an event time
    offset: int | None  # instead of `at` for a topic not seen yet


class Bookmark(TypedDict):
    positions: list[BookmarkPosition]
    bytes: int  # the full id's size, as stored with every row


class Reconnects(TypedDict):
    window_s: int
    total: int
    by_reason: dict[str, int]


IngestState = Literal["live", "catching_up", "paused", "empty"]


class IngestReport(TypedDict):
    state: IngestState  # judged by the API (api/ops.py); the page shows it, doesn't redo it
    newest_event_at: str | None  # the newest event's own time; None with no rows
    last_stored_at: str | None  # when ingest last stored a row (restored rows keep theirs)
    stale_after_s: float  # the age limit `state` is judged with
    lag_ms: LagReport
    bookmark: Bookmark | None
    reconnects: Reconnects


# "from" is a keyword, so these two use the functional form.
FreshnessReport = TypedDict(
    "FreshnessReport",
    {
        "target": float,
        "threshold_s": float,
        "window_days": int,
        "from": str | None,  # None before the first sample
        "to": str | None,
        "minutes": int,
        "fresh": int,
        "stale": int,
        "unmeasured": int,  # no sample: counted against the SLO
        "ratio": float | None,
        "met": bool | None,
        "budget_minutes": int,
        "budget_used": int,
        "full_window": bool,  # False while measuring started less than window_days ago
    },
)

GapReport = TypedDict("GapReport", {"from": str, "to": str, "duration_s": int, "reason": str})


class Gaps(TypedDict):
    window_days: int
    total: int
    recent: list[GapReport]  # the newest, at most 20


class CostReport(TypedDict):
    amount: str  # exactly as AWS returned it
    currency: str  # as AWS reports it
    estimated: bool
    period_start: str
    period_end: str  # exclusive
    fetched_at: str
    source: str


class CostCheck(TypedDict):
    # ok / failed: today's (UTC) attempt is recorded. pending: none recorded today, so a
    # figure in `cost`, if any, is from an earlier day. Never the error text: it can hold
    # an account id.
    today: Literal["ok", "failed", "pending"]
    last_attempt_at: str | None  # the latest attempt on record, any day


class OpsPayload(TypedDict):
    generated_at: str
    ingest: IngestReport
    freshness: FreshnessReport
    gaps: Gaps
    cost: CostReport | None  # this month's latest figure; None until there is one
    cost_check: CostCheck
