"""In-memory pieces of the consumer: the batch being built, recent ids, sequence numbers."""

from __future__ import annotations

import hashlib
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from livedemos.ingest.events import Edit, to_row


@dataclass(frozen=True, slots=True)
class PendingInsert:
    """A sealed batch: exactly what is sent to ClickHouse, and what committing it means.

    Frozen, and nothing mutates the row mappings after sealing, so a retry after an
    ambiguous failure sends the same rows with the same token, and ClickHouse keeps one
    copy whichever attempt lands.
    """

    rows: tuple[Mapping[str, Any], ...]
    token: str
    event_ids: tuple[str, ...]
    bookmark: str  # SSE id of the last row; the resume point once committed
    newest_event_time: datetime

    def __len__(self) -> int:
        return len(self.rows)


class Batch:
    """Rows waiting to be written. Flushed every second or at a row cap, whichever is first.

    The SSE id of the last row is the bookmark. It only becomes the resume point once
    the batch is committed, because the bookmark lives in the rows themselves.
    """

    def __init__(
        self, *, max_rows: int, interval_s: float, clock: Callable[[], float] = time.monotonic
    ):
        self._max_rows = max_rows
        self._interval_s = interval_s
        self._clock = clock
        self._started: float | None = None
        self._rows: list[dict[str, Any]] = []
        self._ids: list[str] = []
        self._id_set: set[str] = set()
        self._last_sse_id: str | None = None
        self._newest: datetime | None = None
        self._day: date | None = None  # UTC day of every row in the batch

    def __len__(self) -> int:
        return len(self._rows)

    def __contains__(self, event_id: object) -> bool:
        return event_id in self._id_set

    def accepts(self, edit: Edit) -> bool:
        """True if adding `edit` keeps the batch inside one partition (one UTC day).

        A ClickHouse INSERT is only atomic within a single partition. A batch that
        spans midnight, or carries a late event from yesterday, would be written as
        two parts, and a failure between them could commit half the batch.
        """
        return self._day is None or _utc_day(edit) == self._day

    def add(self, edit: Edit, *, sse_id: str, ingest_seq: int) -> None:
        if not self.accepts(edit):
            raise ValueError("batch would span two partitions; flush first")
        if self._started is None:
            self._started = self._clock()
            self._day = _utc_day(edit)
        self._rows.append(
            to_row(edit, sse_id=sse_id, ingest_seq=ingest_seq, ingested_at=datetime.now(UTC))
        )
        self._ids.append(edit.event_id)
        self._id_set.add(edit.event_id)
        self._last_sse_id = sse_id
        if self._newest is None or edit.event_time > self._newest:
            self._newest = edit.event_time

    def due(self) -> bool:
        if not self._rows or self._started is None:
            return False
        return (
            len(self._rows) >= self._max_rows or self._clock() - self._started >= self._interval_s
        )

    def time_left(self) -> float:
        """Seconds until the interval flush is due (0 if it already is)."""
        if self._started is None:
            return self._interval_s
        return max(0.0, self._interval_s - (self._clock() - self._started))

    def seal(self) -> PendingInsert:
        if self._last_sse_id is None or self._newest is None:
            raise ValueError("can't seal an empty batch")
        return PendingInsert(
            rows=tuple(self._rows),
            token=_dedup_token(self._ids),
            event_ids=tuple(self._ids),
            bookmark=self._last_sse_id,
            newest_event_time=self._newest,
        )


def _dedup_token(event_ids: Iterable[str]) -> str:
    """Same events, same token, so ClickHouse skips a retried insert.

    Built from event ids, not SSE ids: SSE ids are timestamps, and two different
    events in the same millisecond can share one.
    """
    material = "\n".join(event_ids)
    return hashlib.sha256(material.encode()).hexdigest()[:32]


def _utc_day(edit: Edit) -> date:
    return edit.event_time.astimezone(UTC).date()


class RecentIds:
    """The ids of the most recently ingested events, in ingest order.

    Resuming replays the stream from the bookmark's position, so the events that can
    come back twice are the ones ingested last, whatever their timestamps say. Event
    time is no guide here: Wikimedia's stream interleaves two topics and delivers late
    events, so stream order and `meta.dt` order differ by minutes during a replay.
    """

    def __init__(self, max_ids: int):
        self._max_ids = max_ids
        self._ids: set[str] = set()
        self._order: deque[str] = deque()

    def __contains__(self, event_id: object) -> bool:
        return event_id in self._ids

    def __len__(self) -> int:
        return len(self._ids)

    def extend(self, event_ids: Iterable[str]) -> None:
        for event_id in event_ids:
            if event_id in self._ids:
                continue
            self._ids.add(event_id)
            self._order.append(event_id)
        while len(self._order) > self._max_ids:
            self._ids.discard(self._order.popleft())


class SequenceGenerator:
    """Increasing ids that order rows by ingest, across restarts.

    Seeded with the highest id already committed, so a restart can never hand out a
    smaller one, even if the wall clock moved backwards in between. Wall-clock ns is
    only a floor that keeps ids roughly time-like; ingest is the single writer.
    """

    def __init__(self, start: int = 0, clock: Callable[[], int] = time.time_ns):
        self._clock = clock
        self._last = start

    def next(self) -> int:
        self._last = max(self._last + 1, int(self._clock()))
        return self._last
