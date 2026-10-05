"""Keep the last good live.json in S3, for CloudFront's fallback origin.

Every minute, if the in-memory snapshot is fresh and the data in it is live, write it to
`v1/wikipedia/live.json` in the snapshot bucket, with `status` set to "fallback".
CloudFront only serves that object when the host is down, so a viewer who gets it is
looking at a stopped stream: the widget shows "Paused" at once, with the real age from
the copy's own `as_of`, instead of counting down to `stale_after_s` first.

Nothing here may break the API: S3 errors are logged and counted, then retried on the
next tick.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Any, Protocol

from livedemos.api import metrics
from livedemos.api.snapshot import Snapshot, Snapshotter

log = logging.getLogger(__name__)

KEY = "v1/wikipedia/live.json"
CACHE_CONTROL = "public, max-age=60"


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> Any: ...


class FallbackWriter:
    def __init__(
        self,
        snapshotter: Snapshotter,
        client: S3Client,
        *,
        bucket: str,
        interval_s: float,
        max_snapshot_age_s: float,
        stale_after_s: float,
    ):
        self._snapshotter = snapshotter
        self._client = client
        self._bucket = bucket
        self._interval_s = interval_s
        self._max_snapshot_age_s = max_snapshot_age_s
        self._stale_after_s = stale_after_s

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.write_once()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._interval_s)

    async def write_once(self) -> str:
        """Write the snapshot if it's worth keeping. Returns what happened."""
        snapshot = self._snapshotter.latest
        if snapshot is None:
            outcome = "skipped_no_snapshot"
        else:
            outcome = self._skip_reason(snapshot) or await self._put(snapshot)
        metrics.FALLBACK_WRITES.labels(outcome=outcome).inc()
        return outcome

    def _skip_reason(self, snapshot: Snapshot) -> str | None:
        # Never replace a good fallback with a worse one.
        if time.monotonic() - snapshot.built_at > self._max_snapshot_age_s:
            return "skipped_stale_snapshot"  # can't build new ones (ClickHouse down)
        age = snapshot.last_event_age_s
        if age is None or age > self._stale_after_s:
            return "skipped_paused"  # no data, or ingest has stopped
        return None

    async def _put(self, snapshot: Snapshot) -> str:
        try:
            await asyncio.to_thread(
                self._client.put_object,
                Bucket=self._bucket,
                Key=KEY,
                Body=as_fallback(snapshot.body),
                ContentType="application/json",
                CacheControl=CACHE_CONTROL,
            )
        except Exception:  # any S3 or network failure; try again next tick
            log.warning("fallback snapshot write failed", exc_info=True)
            return "error"
        return "written"


def as_fallback(body: bytes) -> bytes:
    """The same snapshot, marked as the copy served while the host is down."""
    payload = json.loads(body)
    payload["status"] = "fallback"
    return json.dumps(payload, separators=(",", ":")).encode()
