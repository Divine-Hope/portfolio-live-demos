"""GET /v1/ops.json: the Ops tab's payload (ops/report.py), built at most once a minute and
shared by every viewer. CloudFront caches it for what's left of that minute."""

from __future__ import annotations

import asyncio
import json
import time

from livedemos.api.contract import OpsPayload
from livedemos.db.clickhouse import ClickHouseError, Queryable
from livedemos.ops import report


class OpsUnavailable(RuntimeError):
    pass


class OpsService:
    """Builds the payload at most once per `ttl_s`; concurrent requests share one build."""

    def __init__(
        self, db: Queryable, *, policy: report.Policy, ttl_s: float, error_cooldown_s: float
    ):
        self._db = db
        self._policy = policy
        self._ttl_s = ttl_s
        self._error_cooldown_s = error_cooldown_s
        self._lock = asyncio.Lock()
        self._cached: tuple[float, bytes] | None = None
        self._failed_at: float | None = None

    async def get(self) -> tuple[bytes, float]:
        """The payload, and how many seconds ago it was built."""
        async with self._lock:
            now = time.monotonic()
            if self._cached and now - self._cached[0] < self._ttl_s:
                return self._cached[1], now - self._cached[0]
            if self._failed_at is not None and now - self._failed_at < self._error_cooldown_s:
                raise OpsUnavailable("ops data unavailable")
            try:
                payload = await self.build()
            except ClickHouseError as exc:
                self._failed_at = time.monotonic()
                raise OpsUnavailable("ops data unavailable") from exc
            body = json.dumps(payload, separators=(",", ":")).encode()
            self._cached, self._failed_at = (time.monotonic(), body), None
            return body, 0.0

    async def build(self) -> OpsPayload:
        now = time.time()
        rows = await report.collect(self._db, now=now, policy=self._policy)
        return report.assemble(now, rows, self._policy)
