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
    status: Literal["empty", "live", "stale"]
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
    pages_edited: int
    bot_share: float | None
    as_of: str | None
    last_event_age_s: float | None
    query: QueryInfo
