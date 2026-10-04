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


def parse(
    event: Mapping[str, Any],
    *,
    wikis: frozenset[str],
    types: frozenset[str],
    now: datetime | None = None,
) -> Edit | Skip:
    meta = event.get("meta")
    if not isinstance(meta, Mapping):
        return Skip.MALFORMED
    if meta.get("domain") == "canary":
        return Skip.CANARY

    wiki = event.get("wiki")
    if wiki not in wikis:
        return Skip.MALFORMED if not isinstance(wiki, str) else Skip.OTHER_WIKI
    change_type = event.get("type")
    if change_type not in types:
        return Skip.OTHER_TYPE

    try:
        event_id = str(UUID(str(meta["id"])))
        event_time = datetime.fromisoformat(str(meta["dt"]).replace("Z", "+00:00"))
        namespace = int(event["namespace"])
        title = str(event["title"])
    except (KeyError, TypeError, ValueError):
        return Skip.MALFORMED
    if event_time.tzinfo is None or not title:
        return Skip.MALFORMED
    if event_time > (now or datetime.now(UTC)) + MAX_FUTURE:
        return Skip.FUTURE

    return Edit(
        event_id=event_id,
        event_time=event_time,
        wiki=wiki,
        lang=lang_of(wiki),
        type=str(change_type),
        namespace=namespace,
        title=title,
        is_bot=bool(event.get("bot", False)),
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
