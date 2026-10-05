"""Turn raw Wikimedia `recentchange` events into rows we keep, or a reason we skip them.

Pure functions only, so every rule here is unit-tested without a network or database.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID


class Skip(StrEnum):
    CANARY = "canary"  # Wikimedia's synthetic test events
    OTHER_WIKI = "other_wiki"  # not one of the wikis we track
    OTHER_TYPE = "other_type"  # log entries, categorisation, etc.
    MALFORMED = "malformed"  # missing or invalid fields
    FUTURE = "future"  # dated too far ahead; would pin every window to the future


@dataclass(frozen=True, slots=True)
class Edit:
    event_id: str
    event_time: datetime
    wiki: str
    lang: str
    type: str
    namespace: int
    title: str
    is_bot: bool


MAX_FUTURE = timedelta(minutes=5)
# Storage limits. A value ClickHouse would reject must be skipped here: a rejected batch is
# retried unchanged (it may have committed), so one bad row would stall ingest for good.
INT32 = range(-(2**31), 2**31)
MAX_TITLE_BYTES = 1_024  # MediaWiki caps titles at 255 bytes; this is generous


def parse(
    event: object,
    *,
    wikis: frozenset[str],
    types: frozenset[str],
    now: datetime | None = None,
) -> Edit | Skip:
    """Classify one decoded event. Total: any JSON value gets an answer, never an exception.

    The stream is untrusted input. A message that is valid JSON but the wrong shape (null,
    a list, a number in place of a string) is MALFORMED, so one bad event can't stop ingest.
    """
    if not isinstance(event, Mapping):
        return Skip.MALFORMED
    meta = event.get("meta")
    if not isinstance(meta, Mapping):
        return Skip.MALFORMED
    if meta.get("domain") == "canary":
        return Skip.CANARY

    wiki = event.get("wiki")
    if not isinstance(wiki, str):
        return Skip.MALFORMED
    if wiki not in wikis:
        return Skip.OTHER_WIKI
    change_type = event.get("type")
    if not isinstance(change_type, str):
        return Skip.MALFORMED
    if change_type not in types:
        return Skip.OTHER_TYPE

    raw_id, raw_dt = meta.get("id"), meta.get("dt")
    namespace, title, is_bot = event.get("namespace"), event.get("title"), event.get("bot", False)
    if not (isinstance(raw_id, str) and isinstance(raw_dt, str) and isinstance(title, str)):
        return Skip.MALFORMED
    # bool is an int subclass; a namespace of `true` is not namespace 1.
    if not isinstance(namespace, int) or isinstance(namespace, bool) or namespace not in INT32:
        return Skip.MALFORMED
    if not isinstance(is_bot, bool):  # "false" is truthy; don't guess
        return Skip.MALFORMED
    try:
        event_id = str(UUID(raw_id))
        event_time = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
    except ValueError:
        return Skip.MALFORMED
    if (
        event_time.tzinfo is None
        or not title
        or len(title.encode("utf-8", "replace")) > MAX_TITLE_BYTES
    ):
        return Skip.MALFORMED
    if not 1970 < event_time.year < 2106:  # DateTime64 range, as stored
        return Skip.MALFORMED
    if event_time > (now or datetime.now(UTC)) + MAX_FUTURE:
        return Skip.FUTURE

    return Edit(
        event_id=event_id,
        event_time=event_time,
        wiki=wiki,
        lang=lang_of(wiki),
        type=change_type,
        namespace=namespace,
        title=title,
        is_bot=is_bot,
    )


def lang_of(wiki: str) -> str:
    """`enwiki` -> `en`. Only called for wikis we've allowlisted."""
    return wiki.removesuffix("wiki")


def to_row(edit: Edit, *, sse_id: str, ingest_seq: int, ingested_at: datetime) -> dict[str, Any]:
    return {
        "event_id": edit.event_id,
        "event_time": _iso_ms(edit.event_time),
        "ingested_at": _iso_ms(ingested_at),
        "wiki": edit.wiki,
        "lang": edit.lang,
        "type": edit.type,
        "namespace": edit.namespace,
        "title": edit.title,
        "is_bot": edit.is_bot,
        "sse_id": sse_id,
        "ingest_seq": ingest_seq,
    }


def _iso_ms(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
