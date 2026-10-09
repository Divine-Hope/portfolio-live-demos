"""Consume Wikimedia EventStreams over SSE and write edits to ClickHouse.

The loop in one paragraph: connect with the last committed bookmark (or `since` on
first boot), parse each event, drop what we don't keep, add the rest to a batch, and
flush every second. The bookmark only moves after an insert commits. A stream failure
(network, idle socket) throws the unflushed batch away and reconnects from the last
committed bookmark: the stream is the buffer. A failed insert is different, because it
may have committed anyway: that batch is kept, sealed, and retried with the same rows and
the same deduplication token until ClickHouse confirms it, so either attempt can land and
only one copy is kept. Only then does the bookmark move and the stream reconnect.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
from httpx_sse import EventSource, aconnect_sse

from livedemos.clickhouse import ClickHouseError, Database
from livedemos.config import IngestSettings
from livedemos.ingest import metrics
from livedemos.ingest.batch import Batch, PendingInsert, RecentIds, SequenceGenerator
from livedemos.ingest.events import Edit, Skip, parse
from livedemos.ingest.resume import (
    INSERT_QUERY_ID_PREFIX,
    load_resume_state,
    record_gap,
    wait_for_inflight_inserts,
)

log = logging.getLogger(__name__)

_MAX_EXPONENT = 32  # 2**32 seconds is already far past any cap; keeps the float finite
# Reconnects for the Ops tab: written by their own task, off the ingest path, every few
# seconds and with a short timeout, so a slow write can never hold up a batch. Kept in
# memory while ClickHouse is down (often why we reconnected); the oldest go past this many.
_MAX_UNRECORDED_RECONNECTS = 1_000
_RECORD_RECONNECTS_EVERY_S = 5.0
_RECORD_RECONNECTS_TIMEOUT_S = 2.0


class Backoff:
    """Exponential backoff with full jitter."""

    def __init__(self, initial_s: float, max_s: float):
        self._initial = initial_s
        self._max = max_s
        self._attempt = 0

    def reset(self) -> None:
        self._attempt = 0

    def next_delay(self) -> float:
        # Cap the exponent before multiplying: 2**1025 doesn't fit in a float, and a
        # long outage must not crash the code that waits it out.
        ceiling = min(self._max, self._initial * 2 ** min(self._attempt, _MAX_EXPONENT))
        self._attempt += 1
        return random.uniform(0, ceiling)  # noqa: S311 - jitter, not crypto


class Consumer:
    def __init__(self, settings: IngestSettings, ch: Database):
        self._s = settings
        self._ch = ch
        self._seq = SequenceGenerator()
        self._recent = RecentIds(max_ids=settings.seam_ids)
        self._bookmark: str | None = None
        self._bookmark_time: datetime | None = None  # newest event committed
        self._since: datetime | None = None
        self._floor: datetime | None = None  # see ResumeState.floor
        self._pending: PendingInsert | None = None  # sent, outcome unknown
        self._backoff = Backoff(settings.backoff_initial_s, settings.backoff_max_s)
        self._reconnects: deque[dict[str, str]] = deque(maxlen=_MAX_UNRECORDED_RECONNECTS)
        self._reconnects_sealed: tuple[list[dict[str, str]], str] | None = None

    async def run(self, stop: asyncio.Event) -> None:
        metrics.HEARTBEAT.set_to_current_time()
        recorder = asyncio.create_task(self._record_reconnects_until(stop))
        try:
            await self._load_state(stop)
            while not stop.is_set():
                reason = await self._stream_once(stop)
                if stop.is_set():
                    break
                metrics.RECONNECTS.labels(reason=reason).inc()
                self._reconnects.append({"at": datetime.now(UTC).isoformat(), "reason": reason})
                if self._pending is not None:
                    await self._commit_pending(stop)
                delay = self._backoff.next_delay()
                log.warning("reconnecting", extra={"reason": reason, "delay_s": round(delay, 2)})
                await _sleep_or_stop(delay, stop)
        finally:
            recorder.cancel()
            await asyncio.gather(recorder, return_exceptions=True)
            await self._record_reconnects()  # one last, bounded try before exiting

    async def _load_state(self, stop: asyncio.Event) -> None:
        backoff = Backoff(self._s.backoff_initial_s, self._s.backoff_max_s)
        while not stop.is_set():
            metrics.HEARTBEAT.set_to_current_time()
            try:
                # An insert from a killed predecessor may still be running. Read the
                # bookmark only after it has finished, however long that takes: reading
                # earlier could replay its rows under another token. The writer profile's
                # max_execution_time (30 s) ends it in practice; this only warns.
                if await wait_for_inflight_inserts(self._ch, timeout_s=self._s.inflight_warn_s):
                    await self._load_state_once()
                    return
                metrics.INFLIGHT_WAITS.inc()
                log.warning("an earlier insert is still running; waiting for it")
                continue
            except ClickHouseError as exc:
                log.warning("loading resume state failed", extra={"error": str(exc)})
            await _sleep_or_stop(backoff.next_delay(), stop)

    async def _load_state_once(self) -> None:
        now = datetime.now(UTC)
        state = await load_resume_state(
            self._ch,
            now=now,
            retention=timedelta(seconds=self._s.retention_s),
            lookback=timedelta(seconds=self._s.first_boot_lookback_s),
            seam_ids=self._s.seam_ids,
        )
        self._bookmark = state.bookmark
        self._bookmark_time = state.newest_event_time
        self._since = state.since
        self._floor = state.floor
        self._seq = SequenceGenerator(start=state.last_seq)
        self._recent.extend(reversed(state.seam_ids))  # oldest first, so pruning drops the oldest
        if state.gap is not None:
            await record_gap(
                self._ch, gap_from=state.gap[0], gap_to=state.gap[1], reason="retention"
            )
            metrics.GAPS.inc()

    async def _expire_old_bookmark(self) -> None:
        """Before reconnecting: is the bookmark still inside the source's retention?

        A long outage can pass while this process stays up. Resuming from a position the
        source no longer has would silently start at its oldest event, so start fresh
        and record the gap instead.
        """
        if self._bookmark is None or self._bookmark_time is None:
            return
        now = datetime.now(UTC)
        if self._bookmark_time >= now - timedelta(seconds=self._s.retention_s):
            return
        since = now - timedelta(seconds=self._s.first_boot_lookback_s)
        log.warning("bookmark aged out", extra={"newest": self._bookmark_time.isoformat()})
        # From the start of its minute: ingest stopped part way through it.
        gap_from = self._bookmark_time.replace(second=0, microsecond=0)
        await record_gap(self._ch, gap_from=gap_from, gap_to=since, reason="retention")
        metrics.GAPS.inc()
        self._bookmark, self._bookmark_time, self._since = None, None, since

    async def _stream_once(self, stop: asyncio.Event) -> str:
        """Hold one connection until something goes wrong. Returns the reason it ended."""
        try:
            await self._expire_old_bookmark()
        except ClickHouseError as exc:
            log.error("recording a gap failed", extra={"error": str(exc)})
            return "clickhouse"
        headers = {"User-Agent": self._s.user_agent, "Accept": "text/event-stream"}
        params: dict[str, str] = {}
        if self._bookmark:
            headers["Last-Event-ID"] = self._bookmark
        elif self._since:
            params["since"] = self._since.isoformat().replace("+00:00", "Z")

        timeout = httpx.Timeout(connect=10.0, read=self._s.idle_timeout_s, write=10.0, pool=10.0)
        batch = self._new_batch()
        try:
            async with (
                httpx.AsyncClient(timeout=timeout) as client,
                aconnect_sse(
                    client, "GET", self._s.stream_url, headers=headers, params=params
                ) as source,
            ):
                source.response.raise_for_status()
                metrics.CONNECTED.set(1)
                log.info("connected", extra={"resumed": bool(self._bookmark)})
                # A reader task parses and filters into a bounded queue, so the flush timer
                # fires even when the stream goes quiet, a slow insert pushes back on the
                # socket, and only rows we keep take up memory.
                queue: asyncio.Queue[_Item] = asyncio.Queue(maxsize=self._s.flush_max_rows * 2)
                reader = asyncio.create_task(_read_into(source, queue, self._parse))
                try:
                    while not stop.is_set():
                        metrics.HEARTBEAT.set_to_current_time()
                        metrics.QUEUE_DEPTH.set(queue.qsize())
                        wait_s = batch.time_left() if len(batch) else self._s.flush_interval_s
                        try:
                            item = await asyncio.wait_for(queue.get(), timeout=wait_s)
                        except TimeoutError:
                            item = None
                        if isinstance(item, BaseException):
                            raise item
                        if item is _EOF:
                            return "eof"
                        if isinstance(item, tuple):
                            edit, sse_id = item
                            if self._is_new(edit, batch):
                                if not batch.accepts(edit):  # one partition per INSERT
                                    await self._flush(batch)
                                    batch = self._new_batch()
                                batch.add(edit, sse_id=sse_id, ingest_seq=self._seq.next())
                        if batch.due():
                            await self._flush(batch)
                            batch = self._new_batch()
                    return "stop"
                finally:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
        except httpx.ReadTimeout:
            return "idle"
        except httpx.HTTPStatusError as exc:
            log.warning("stream refused", extra={"status": exc.response.status_code})
            return "http_status"
        except httpx.HTTPError as exc:
            log.warning("stream error", extra={"error": repr(exc)})
            return "network"
        except ClickHouseError as exc:
            # Backpressure: stop reading. Wikimedia keeps the events; we'll come back for them.
            log.error("insert failed, disconnecting", extra={"error": str(exc)})
            return "clickhouse"
        finally:
            metrics.CONNECTED.set(0)
            metrics.QUEUE_DEPTH.set(0)
            if len(batch) and self._pending is None:
                log.info("dropping unsent batch, it will be replayed", extra={"rows": len(batch)})

    def _parse(self, data: str, sse_id: str) -> Edit | None:
        """Parse one message. Returns an edit to keep, or None (counted by reason)."""
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            metrics.EVENTS.labels(outcome=Skip.MALFORMED).inc()
            return None
        result = parse(event, wikis=self._s.wiki_set, types=self._s.type_set)
        if isinstance(result, Skip):
            metrics.EVENTS.labels(outcome=result.value).inc()
            return None
        if not sse_id:
            metrics.EVENTS.labels(outcome=Skip.MALFORMED).inc()
            return None
        # Older than what this run may add: minutes the rollup already counts (rebuilt
        # from the archive, or kept from before raw rows expired). Counting it would double
        # it. Only set when ingest started without a bookmark.
        if self._floor and result.event_time < self._floor:
            metrics.EVENTS.labels(outcome="before_floor").inc()
            return None
        return result

    def _is_new(self, edit: Edit, batch: Batch) -> bool:
        if edit.event_id in self._recent or edit.event_id in batch:
            metrics.EVENTS.labels(outcome="duplicate").inc()
            return False
        return True

    async def _flush(self, batch: Batch) -> None:
        pending = batch.seal()
        self._pending = pending  # if this raises, the outcome is unknown: keep it
        await self._insert(pending)
        self._committed(pending)

    async def _record_reconnects_until(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await _sleep_or_stop(_RECORD_RECONNECTS_EVERY_S, stop)
            await self._record_reconnects()

    async def _record_reconnects(self) -> None:
        """Write the reconnects not yet recorded, for the Ops tab. Never raises.

        Like a batch of edits: the rows are sealed with a token first and retried unchanged
        until ClickHouse confirms them, so an insert that landed but timed out isn't counted
        twice. Reconnects that happen meanwhile wait for the next batch.
        """
        if self._reconnects_sealed is None:
            if not self._reconnects:
                return
            self._reconnects_sealed = (list(self._reconnects), f"reconnects-{uuid4().hex}")
            self._reconnects.clear()
        rows, token = self._reconnects_sealed
        try:
            # Giving up on the reply doesn't stop the insert on the server. The same query id
            # makes a retry wait its turn instead of running alongside it.
            await asyncio.wait_for(
                self._ch.insert("ingest_reconnects", rows, dedup_token=token, query_id=token),
                timeout=_RECORD_RECONNECTS_TIMEOUT_S,
            )
        except (ClickHouseError, TimeoutError) as exc:
            log.warning("recording reconnects failed", extra={"error": repr(exc)})
            return
        self._reconnects_sealed = None

    async def _insert(self, pending: PendingInsert) -> None:
        started = time.perf_counter()
        try:
            await self._ch.insert(
                "wiki_edits",
                pending.rows,
                dedup_token=pending.token,
                query_id=f"{INSERT_QUERY_ID_PREFIX}{pending.token}",
            )
        except ClickHouseError:
            metrics.BATCHES.labels(result="error").inc()
            raise
        metrics.INSERT_SECONDS.observe(time.perf_counter() - started)
        metrics.BATCHES.labels(result="ok").inc()

    def _committed(self, pending: PendingInsert) -> None:
        # Committed. Only now does the bookmark move.
        self._pending = None
        self._bookmark = pending.bookmark
        self._since = None
        if self._bookmark_time is None or pending.newest_event_time > self._bookmark_time:
            self._bookmark_time = pending.newest_event_time
        self._recent.extend(pending.event_ids)
        metrics.BATCH_ROWS.observe(len(pending))
        metrics.ROWS_WRITTEN.inc(len(pending))
        metrics.EVENTS.labels(outcome="kept").inc(len(pending))
        newest_ts = pending.newest_event_time.timestamp()
        metrics.LAST_EVENT_TS.set(newest_ts)
        metrics.LAST_COMMIT_TS.set_to_current_time()
        metrics.LAG_SECONDS.set(max(0.0, time.time() - newest_ts))
        self._backoff.reset()

    def _new_batch(self) -> Batch:
        return Batch(max_rows=self._s.flush_max_rows, interval_s=self._s.flush_interval_s)

    async def _commit_pending(self, stop: asyncio.Event) -> None:
        """Retry the batch whose insert failed, unchanged, until ClickHouse confirms it.

        The failure may have been a timeout on an insert that committed, or is still
        running. Same rows and same token mean ClickHouse keeps exactly one copy, whichever
        attempt lands. While the first attempt is still running, its query_id is taken,
        so a retry is refused until it finishes.
        """
        pending = self._pending
        if pending is None:
            return
        backoff = Backoff(self._s.backoff_initial_s, self._s.backoff_max_s)
        while not stop.is_set():
            metrics.HEARTBEAT.set_to_current_time()
            await _sleep_or_stop(backoff.next_delay(), stop)
            if stop.is_set():
                break
            try:
                await self._insert(pending)
            except ClickHouseError as exc:
                log.warning("retrying failed batch", extra={"error": str(exc)})
                continue
            self._committed(pending)
            log.info("failed batch confirmed", extra={"rows": len(pending)})
            return
        # Stopping with the outcome unknown: the next start waits for in-flight inserts
        # and reads the bookmark from what actually committed.


class _EndOfStream:
    pass


_EOF = _EndOfStream()
_Item = tuple[Edit, str] | _EndOfStream | Exception


async def _read_into(
    source: EventSource,
    queue: asyncio.Queue[_Item],
    parse_one: Callable[[str, str], Edit | None],
) -> None:
    try:
        async for message in source.aiter_sse():
            if not message.data:
                continue
            edit = parse_one(message.data, message.id)
            if edit is not None:
                await queue.put((edit, message.id))
        await queue.put(_EOF)
    except Exception as exc:  # handed to the consumer loop, which decides what it means
        await queue.put(exc)


async def _sleep_or_stop(delay: float, stop: asyncio.Event) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=delay)
