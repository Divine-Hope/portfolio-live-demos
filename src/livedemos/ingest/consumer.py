"""Consume Wikimedia EventStreams over SSE and write edits to ClickHouse.

The loop in one paragraph: connect with the last committed bookmark (or `since` on
first boot), parse each event, drop what we don't keep, add the rest to a batch, and
flush every second. The bookmark only moves after an insert commits. Any failure
(network, idle socket, ClickHouse down) closes the connection, throws the unflushed
batch away and reconnects from the last committed bookmark. The stream is the buffer.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from datetime import UTC, datetime, timedelta

import httpx
from httpx_sse import EventSource, ServerSentEvent, aconnect_sse

from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import IngestSettings
from livedemos.ingest import metrics
from livedemos.ingest.batch import Batch, RecentIds, SequenceGenerator
from livedemos.ingest.events import Edit, Skip, parse
from livedemos.ingest.resume import load_resume_state, record_gap

log = logging.getLogger(__name__)


class Backoff:
    """Exponential backoff with full jitter."""

    def __init__(self, initial_s: float, max_s: float):
        self._initial = initial_s
        self._max = max_s
        self._attempt = 0

    def reset(self) -> None:
        self._attempt = 0

    def next_delay(self) -> float:
        ceiling = min(self._max, self._initial * 2**self._attempt)
        self._attempt += 1
        return random.uniform(0, ceiling)  # noqa: S311 - jitter, not crypto


class Consumer:
    def __init__(self, settings: IngestSettings, ch: ClickHouse):
        self._s = settings
        self._ch = ch
        self._seq = SequenceGenerator()
        self._recent = RecentIds(max_ids=settings.seam_ids)
        self._bookmark: str | None = None
        self._since: datetime | None = None
        self._backoff = Backoff(settings.backoff_initial_s, settings.backoff_max_s)

    async def run(self, stop: asyncio.Event) -> None:
        metrics.HEARTBEAT.set_to_current_time()
        await self._load_state()
        while not stop.is_set():
            reason = await self._stream_once(stop)
            if stop.is_set():
                break
            metrics.RECONNECTS.labels(reason=reason).inc()
            if reason == "clickhouse":
                await self._recover_from_clickhouse_error(stop)
            delay = self._backoff.next_delay()
            log.warning("reconnecting", extra={"reason": reason, "delay_s": round(delay, 2)})
            await _sleep_or_stop(delay, stop)

    async def _load_state(self) -> None:
        now = datetime.now(UTC)
        state = await load_resume_state(
            self._ch,
            now=now,
            retention=timedelta(seconds=self._s.retention_s),
            lookback=timedelta(seconds=self._s.first_boot_lookback_s),
            seam_ids=self._s.seam_ids,
        )
        self._bookmark = state.bookmark
        self._since = state.since
        self._recent.extend(reversed(state.seam_ids))  # oldest first, so pruning drops the oldest
        if state.gap_from is not None:
            await record_gap(self._ch, gap_from=state.gap_from, gap_to=now, reason="retention")
            metrics.GAPS.inc()

    async def _stream_once(self, stop: asyncio.Event) -> str:
        """Hold one connection until something goes wrong. Returns the reason it ended."""
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
                # A reader task feeds a bounded queue, so the flush timer fires even when
                # the stream goes quiet, and a slow insert pushes back on the socket.
                queue: asyncio.Queue[_Item] = asyncio.Queue(maxsize=self._s.flush_max_rows * 2)
                reader = asyncio.create_task(_read_into(source, queue))
                try:
                    while not stop.is_set():
                        metrics.HEARTBEAT.set_to_current_time()
                        wait_s = batch.time_left() if len(batch) else self._s.flush_interval_s
                        try:
                            item = await asyncio.wait_for(queue.get(), timeout=wait_s)
                        except TimeoutError:
                            item = None
                        if isinstance(item, BaseException):
                            raise item
                        if item is _EOF:
                            return "eof"
                        if isinstance(item, ServerSentEvent) and item.data:
                            edit = self._parse_new(item.data, item.id, batch)
                            if edit is not None:
                                if not batch.accepts(edit):  # one partition per INSERT
                                    await self._flush(batch)
                                    batch = self._new_batch()
                                batch.add(edit, sse_id=item.id, ingest_seq=self._seq.next())
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
            if len(batch):
                log.info(
                    "dropping unflushed batch, it will be replayed", extra={"rows": len(batch)}
                )

    def _parse_new(self, data: str, sse_id: str, batch: Batch) -> Edit | None:
        """Parse one message. Returns an edit we haven't stored yet, or None."""
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            metrics.EVENTS.labels(outcome=Skip.MALFORMED).inc()
            return None
        result = parse(event, wikis=self._s.wiki_set, types=self._s.type_set)
        if isinstance(result, Skip):
            metrics.EVENTS.labels(outcome=result.value).inc()
            return None
        if result.event_id in self._recent or result.event_id in batch or not sse_id:
            metrics.EVENTS.labels(outcome="duplicate").inc()
            return None
        return result

    async def _flush(self, batch: Batch) -> None:
        started = time.perf_counter()
        try:
            await self._ch.insert(
                "wiki_edits",
                batch.rows,
                dedup_token=batch.dedup_token(),
            )
        except ClickHouseError:
            metrics.BATCHES.labels(result="error").inc()
            raise
        metrics.INSERT_SECONDS.observe(time.perf_counter() - started)
        metrics.BATCHES.labels(result="ok").inc()
        metrics.BATCH_ROWS.observe(len(batch))
        metrics.ROWS_WRITTEN.inc(len(batch))
        metrics.EVENTS.labels(outcome="kept").inc(len(batch))

        # Committed. Only now does the bookmark move.
        self._bookmark = batch.last_sse_id
        self._since = None
        self._recent.extend(event_id for event_id, _ in batch.event_ids)
        newest_ts = max(ts for _, ts in batch.event_ids)
        metrics.LAST_EVENT_TS.set(newest_ts)
        metrics.LAG_SECONDS.set(max(0.0, time.time() - newest_ts))
        self._backoff.reset()

    def _new_batch(self) -> Batch:
        return Batch(max_rows=self._s.flush_max_rows, interval_s=self._s.flush_interval_s)

    async def _recover_from_clickhouse_error(self, stop: asyncio.Event) -> None:
        """Wait for ClickHouse, then re-read the bookmark from what was actually committed.

        An insert that "failed" (a timeout, say) may still have committed. Its rows
        carry their own bookmark, so reloading from the table is the only way to know
        where we really are. Without this, the replay would write those rows again.
        """
        backoff = Backoff(self._s.backoff_initial_s, self._s.backoff_max_s)
        while not stop.is_set():
            metrics.HEARTBEAT.set_to_current_time()
            if await self._ch.ping():
                try:
                    await self._load_state()
                    return
                except ClickHouseError as exc:
                    log.warning("reloading state failed", extra={"error": str(exc)})
            await _sleep_or_stop(backoff.next_delay(), stop)


class _EndOfStream:
    pass


_EOF = _EndOfStream()
_Item = ServerSentEvent | _EndOfStream | Exception


async def _read_into(source: EventSource, queue: asyncio.Queue[_Item]) -> None:
    try:
        async for message in source.aiter_sse():
            await queue.put(message)
        await queue.put(_EOF)
    except Exception as exc:  # handed to the consumer loop, which decides what it means
        await queue.put(exc)


async def _sleep_or_stop(delay: float, stop: asyncio.Event) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=delay)
