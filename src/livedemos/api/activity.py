"""The "Query it" endpoint: a real ad hoc query, with ClickHouse's own timing attached.

Parameters are allowlisted, results are cached for a few seconds in-process, and
CloudFront caches them again in front. Visitors can't make ClickHouse do anything
we haven't already decided it should do.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from livedemos.api import metrics, queries
from livedemos.api.snapshot import iso
from livedemos.clickhouse import ClickHouse

WINDOWS = {"5m": 300, "1h": 3_600, "24h": 86_400}


class BadRequest(ValueError):
    pass


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
    def __init__(self, ch: ClickHouse, *, ttl_s: float):
        self._ch = ch
        self._ttl_s = ttl_s
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._lock = asyncio.Lock()

    async def get(self, req: ActivityRequest) -> dict[str, Any]:
        hit = self._fresh(req.key)
        if hit is not None:
            metrics.ACTIVITY_QUERIES.labels(cache="hit").inc()
            return {**hit, "query": {**hit["query"], "cache": "hit"}}
        async with self._lock:  # one miss at a time: no thundering herd on ClickHouse
            hit = self._fresh(req.key)
            if hit is not None:
                metrics.ACTIVITY_QUERIES.labels(cache="hit").inc()
                return {**hit, "query": {**hit["query"], "cache": "hit"}}
            payload = await self._compute(req)
            self._cache[req.key] = (time.monotonic() + self._ttl_s, payload)
            metrics.ACTIVITY_QUERIES.labels(cache="miss").inc()
            return payload

    def _fresh(self, key: str) -> dict[str, Any] | None:
        entry = self._cache.get(key)
        if entry and entry[0] > time.monotonic():
            return entry[1]
        return None

    async def _compute(self, req: ActivityRequest) -> dict[str, Any]:
        head = await self._ch.query(queries.NEWEST)
        newest_ms = queries.newest_ms(head.rows)
        now = time.time()
        base: dict[str, Any] = {
            "lang": ",".join(req.langs),
            "window": req.window,
            "generated_at": iso(now),
        }
        if newest_ms is None:
            return {
                **base,
                "edits": 0,
                "pages_edited": 0,
                "bot_share": None,
                "as_of": None,
                "last_event_age_s": None,
                "query": {"elapsed_ms": 0.0, "rows_read": 0, "bytes_read": 0, "cache": "miss"},
            }

        result = await self._ch.query(
            queries.WINDOW_TOTALS,
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
            **base,
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
