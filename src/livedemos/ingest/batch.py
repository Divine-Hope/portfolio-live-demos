"""In-memory pieces of the consumer: the batch being built, recent ids, sequence numbers."""

from __future__ import annotations

import hashlib
import time
from collections import deque
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
from typing import Any

from livedemos.ingest.events import Edit, to_row


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
        self.rows: list[dict[str, Any]] = []
        self.event_ids: list[tuple[str, float]] = []
        self._id_set: set[str] = set()
        self.first_sse_id: str | None = None
        self.last_sse_id: str | None = None
        self.day: date | None = None  # UTC day of every row in the batch

    def __len__(self) -> int:
        return len(self.rows)

    def __contains__(self, event_id: object) -> bool:
        return event_id in self._id_set

    def accepts(self, edit: Edit) -> bool:
        """True if adding `edit` keeps the batch inside one partition (one UTC day).

        A ClickHouse INSERT is only atomic within a single partition. A batch that
        spans midnight, or carries a late event from yesterday, would be written as
        two parts, and a failure between them could commit half the batch.
        """
        return self.day is None or _utc_day(edit) == self.day

    def add(self, edit: Edit, *, sse_id: str, ingest_seq: int) -> None:
        if not self.accepts(edit):
            raise ValueError("batch would span two partitions; flush first")
        if self._started is None:
            self._started = self._clock()
            self.first_sse_id = sse_id
            self.day = _utc_day(edit)
        self.rows.append(
            to_row(edit, sse_id=sse_id, ingest_seq=ingest_seq, ingested_at=datetime.now(UTC))
        )
        self.event_ids.append((edit.event_id, edit.event_time.timestamp()))
        self._id_set.add(edit.event_id)
        self.last_sse_id = sse_id

    def due(self) -> bool:
        if not self.rows or self._started is None:
            return False
        return len(self.rows) >= self._max_rows or self._clock() - self._started >= self._interval_s

    def time_left(self) -> float:
        """Seconds until the interval flush is due (0 if it already is)."""
        if self._started is None:
            return self._interval_s
        return max(0.0, self._interval_s - (self._clock() - self._started))

    def dedup_token(self) -> str:
        """Same events, same token, so ClickHouse skips a retried insert.

        Built from event ids, not SSE ids: SSE ids are timestamps, and two different
        events in the same millisecond can share one.
        """
        material = "\n".join(event_id for event_id, _ in self.event_ids)
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
    """Monotonic ids that keep increasing across restarts (anchored to wall-clock ns)."""

    def __init__(self, clock: Callable[[], int] = time.time_ns):
        self._clock = clock
        self._last = 0

    def next(self) -> int:
        self._last = max(self._last + 1, int(self._clock()))
        return self._last
