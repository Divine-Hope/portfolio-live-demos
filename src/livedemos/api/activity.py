"""The "Query it" endpoint: a real ad hoc query, with ClickHouse's own timing attached.

Parameters are allowlisted, results are cached for a few seconds in-process, and
CloudFront caches them again in front. Visitors can't make ClickHouse do anything
we haven't already decided it should do, or make it do it more often than we allow:

- one query per cache key at a time; concurrent misses wait for the same result,
- at most `max_concurrency` queries run at once, and at most `max_pending` are admitted
  (running or queued); a miss beyond that gets a 503 straight away,
- no request waits longer than `wait_s`. Its query keeps running for the others waiting
  on it, and to fill the cache, so it still counts against `max_pending` until it ends,
- a failed query is remembered for `error_cooldown_s`, so a struggling ClickHouse
  isn't retried by every visitor.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass

from livedemos.api import metrics, queries
from livedemos.api.contract import ActivityPayload, iso
from livedemos.clickhouse import ClickHouseError, Queryable

WINDOWS = {"5m": 300, "1h": 3_600, "24h": 86_400, "3d": 259_200, "7d": 604_800}
# Longer windows read per-minute tables instead of raw rows (queries.py).
ROLLUP_WINDOWS = frozenset({"3d", "7d"})


class BadRequest(ValueError):
    pass


class Unavailable(RuntimeError):
    """Can't answer right now (ClickHouse failing, or too busy). Maps to a 503."""


@dataclass(frozen=True, slots=True)
class ActivityRequest:
    langs: tuple[str, ...]
    window: str

    @property
    def key(self) -> str:
        return f"{','.join(self.langs)}|{self.window}"


def parse_request(
    lang: str | None, window: str | None, *, allowed: Sequence[str]
) -> ActivityRequest:
    raw_langs = (lang or "all").strip().lower()
    if raw_langs == "all":
        langs = tuple(allowed)
    else:
        requested = {part.strip() for part in raw_langs.split(",") if part.strip()}
        unknown = requested - set(allowed)
        if not requested or unknown:
            raise BadRequest(f"lang must be 'all' or a comma list of: {', '.join(allowed)}")
        langs = tuple(code for code in allowed if code in requested)  # stable order
    win = (window or "5m").strip().lower()
    if win not in WINDOWS:
        raise BadRequest(f"window must be one of: {', '.join(WINDOWS)}")
    return ActivityRequest(langs=langs, window=win)


class ActivityService:
    def __init__(
        self,
        db: Queryable,
        *,
        ttl_s: float,
        max_concurrency: int = 2,
        max_pending: int = 4,
        wait_s: float = 5.0,
        error_cooldown_s: float = 5.0,
    ):
        self._db = db
        self._ttl_s = ttl_s
        self._wait_s = wait_s
        self._error_cooldown_s = error_cooldown_s
        self._cache: dict[str, tuple[float, ActivityPayload]] = {}
        self._failed_until: dict[str, float] = {}
        self._inflight: dict[str, asyncio.Task[ActivityPayload]] = {}
        self._slots = asyncio.Semaphore(max_concurrency)
        if max_pending < max_concurrency:
            raise ValueError("max_pending must be at least max_concurrency")
        self._max_pending = max_pending

    async def get(self, req: ActivityRequest) -> ActivityPayload:
        now = time.monotonic()
        entry = self._cache.get(req.key)
        if entry and entry[0] > now:
            metrics.ACTIVITY_QUERIES.labels(cache="hit").inc()
            return {**entry[1], "query": {**entry[1]["query"], "cache": "hit"}}
        if self._failed_until.get(req.key, 0.0) > now:
            metrics.ACTIVITY_QUERIES.labels(cache="cooldown").inc()
            raise Unavailable("query failed recently")

        task = self._inflight.get(req.key)
        if task is None:
            if len(self._inflight) >= self._max_pending:
                metrics.ACTIVITY_QUERIES.labels(cache="shed").inc()
                raise Unavailable("busy")
            task = asyncio.create_task(self._fill(req))
            self._inflight[req.key] = task
            task.add_done_callback(lambda t: self._settle(req.key, t))
            metrics.ACTIVITY_QUERIES.labels(cache="miss").inc()
        else:
            metrics.ACTIVITY_QUERIES.labels(cache="coalesced").inc()
        try:
            # shield: one impatient visitor timing out mustn't cancel everyone's query.
            return await asyncio.wait_for(asyncio.shield(task), timeout=self._wait_s)
        except TimeoutError as exc:
            metrics.ACTIVITY_QUERIES.labels(cache="timeout").inc()
            raise Unavailable("busy") from exc
        except ClickHouseError as exc:
            raise Unavailable("query failed") from exc

    async def aclose(self) -> None:
        """Cancel queries still running at shutdown."""
        tasks = list(self._inflight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _settle(self, key: str, task: asyncio.Task[ActivityPayload]) -> None:
        self._inflight.pop(key, None)
        if not task.cancelled():
            task.exception()  # retrieved here, so a result nobody waited for isn't "lost"

    async def _fill(self, req: ActivityRequest) -> ActivityPayload:
        async with self._slots:
            try:
                payload = await self._compute(req)
            except ClickHouseError:
                self._failed_until[req.key] = time.monotonic() + self._error_cooldown_s
                metrics.ACTIVITY_FAILURES.inc()
                raise
        self._failed_until.pop(req.key, None)
        self._cache[req.key] = (time.monotonic() + self._ttl_s, payload)
        return payload

    async def _compute(self, req: ActivityRequest) -> ActivityPayload:
        head = await self._db.query(queries.NEWEST)
        newest_ms = queries.newest_ms(head.rows)
        now = time.time()
        lang, generated_at = ",".join(req.langs), iso(now)
        if newest_ms is None:
            return {
                "lang": lang,
                "window": req.window,
                "generated_at": generated_at,
                "edits": 0,
                "pages_edited": 0,
                "bot_share": None,
                "as_of": None,
                "last_event_age_s": None,
                "query": {"elapsed_ms": 0.0, "rows_read": 0, "bytes_read": 0, "cache": "miss"},
            }

        from_rollup = req.window in ROLLUP_WINDOWS
        result = await self._db.query(
            queries.WINDOW_TOTALS_FROM_ROLLUP if from_rollup else queries.WINDOW_TOTALS,
            params={
                "to_ms": newest_ms,
                "window_s": WINDOWS[req.window],
                "langs": list(req.langs),
            },
        )
        edits = sum(int(r["edits"]) for r in result.rows)
        bots = sum(int(r["bot_edits"]) for r in result.rows)
        newest = newest_ms / 1000
        return {
            "lang": lang,
            "window": req.window,
            "generated_at": generated_at,
            "edits": edits,
            "pages_edited": sum(int(r["pages"]) for r in result.rows),
            "bot_share": round(bots / edits, 4) if edits else None,
            "as_of": iso(newest),
            "last_event_age_s": round(max(0.0, now - newest), 3),
            "query": {
                "elapsed_ms": result.stats.elapsed_ms,
                "rows_read": result.stats.rows_read,
                "bytes_read": result.stats.bytes_read,
                "cache": "miss",
            },
        }
