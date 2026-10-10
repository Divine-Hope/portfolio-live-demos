"""Compute the widget's data once per second and keep it in memory.

Every viewer gets the same bytes. One viewer or five hundred, ClickHouse does the
same work. In production CloudFront caches this for a second and fans it out.

Consistency: every query is bounded above by one watermark, the newest event time read
first (`to_ms`): raw-row queries by event time, the per-minute chart by using the rollup
only for completed minutes and raw rows for the current one. The queries run
concurrently, so a late event (dated before the watermark, inserted after it was read)
can show up in one query and not another. Inserts are one-second batches, so that's at
most one batch of late events, and the next snapshot agrees with itself again.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from livedemos.aio import sleep_unless_stopped
from livedemos.api import metrics, queries
from livedemos.api.contract import Article, LangSummary, LivePayload, MinuteBucket, article_url
from livedemos.dates import iso
from livedemos.db.clickhouse import Queryable

log = logging.getLogger(__name__)

WINDOW_S = 300  # "last 5 minutes"
MINUTES = 60  # "edits per minute, last hour"
TOP_N = 5


@dataclass(frozen=True, slots=True)
class Snapshot:
    body: bytes  # pre-serialised JSON, served as-is
    built_at: float  # time.monotonic() when built
    last_event_age_s: float | None


def assemble(
    *,
    now: float,
    newest: float | None,
    langs: Sequence[str],
    window_rows: Iterable[Mapping[str, Any]],
    top_rows: Iterable[Mapping[str, Any]],
    minute_rows: Iterable[Mapping[str, Any]],
    lag: Mapping[str, Any] | None,
    gaps: Iterable[Mapping[str, Any]],
    stale_after_s: float,
) -> LivePayload:
    """Pure function: query rows in, widget payload out."""
    if newest is None:
        return {
            "dataset": "wikipedia",
            "status": "empty",
            "computed_at": iso(now),
            "as_of": None,
            "last_event_age_s": None,
            "stale_after_s": stale_after_s,
            "langs": {},
        }

    age = max(0.0, now - newest)
    groups = [*langs, "all"]
    payload_langs: dict[str, LangSummary] = {}

    totals = {lang: {"edits": 0, "pages": 0, "bot_edits": 0} for lang in groups}
    for row in window_rows:
        lang = str(row["lang"])
        if lang not in totals:
            continue
        for key in ("edits", "pages", "bot_edits"):
            totals[lang][key] += int(row[key])
            totals["all"][key] += int(row[key])  # pages differ across wikis, so sums hold

    tops: dict[str, list[Article]] = {lang: [] for lang in groups}
    for row in top_rows:
        lang = str(row["lang"])
        if lang not in tops or lang == "all":
            continue
        item: Article = {
            "title": str(row["title"]),
            "lang": lang,
            "edits": int(row["edits"]),
            "url": article_url(lang, str(row["title"])),
        }
        tops[lang].append(item)
        tops["all"].append(item)
    for lang in groups:
        tops[lang] = sorted(tops[lang], key=lambda i: (-i["edits"], i["title"]))[:TOP_N]

    minutes = _per_minute(newest=newest, langs=langs, rows=minute_rows, gaps=gaps)

    for lang in groups:
        t = totals[lang]
        payload_langs[lang] = {
            "edits_5m": t["edits"],
            "pages_5m": t["pages"],
            "bot_share_5m": round(t["bot_edits"] / t["edits"], 4) if t["edits"] else None,
            "top_articles": tops[lang],
            "per_minute": minutes[lang],
        }

    return {
        "dataset": "wikipedia",
        "status": "live" if age <= stale_after_s else "stale",
        "computed_at": iso(now),
        "as_of": iso(newest),
        "last_event_age_s": round(age, 3),
        "stale_after_s": stale_after_s,
        "ingest_lag_ms": {
            "p50": _int_or_none(lag, "p50_ms"),
            "p95": _int_or_none(lag, "p95_ms"),
        },
        "window_s": WINDOW_S,
        "langs": payload_langs,
    }


def _per_minute(
    *,
    newest: float,
    langs: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
    gaps: Iterable[Mapping[str, Any]],
) -> dict[str, list[MinuteBucket]]:
    """60 one-minute buckets ending with the newest event's minute.

    A minute with no rows at all, or inside a recorded gap, is `null`: we don't know,
    so we don't draw a zero. The last bucket is still filling up and is marked partial.
    """
    last_minute = int(newest // 60) * 60
    buckets = [last_minute - 60 * i for i in range(MINUTES - 1, -1, -1)]
    counts: dict[int, dict[str, int]] = {}
    for row in rows:
        lang = str(row["lang"])
        if lang in langs:
            counts.setdefault(int(row["minute_s"]), {})[lang] = int(row["edits"])
    gap_ranges = [(int(g["from_ms"]) / 1000, int(g["to_ms"]) / 1000) for g in gaps]

    out: dict[str, list[MinuteBucket]] = {lang: [] for lang in [*langs, "all"]}
    for minute in buckets:
        known = minute in counts and not any(a < minute + 60 and b > minute for a, b in gap_ranges)
        per_lang = counts.get(minute, {})
        partial = minute == last_minute
        for lang in langs:
            value = per_lang.get(lang, 0) if known else None
            out[lang].append({"t": minute, "edits": value, "partial": partial})
        all_value = sum(per_lang.values()) if known else None
        out["all"].append({"t": minute, "edits": all_value, "partial": partial})
    return out


def _int_or_none(row: Mapping[str, Any] | None, key: str) -> int | None:
    if not row or row.get(key) is None:
        return None
    try:
        return int(float(row[key]))
    except (TypeError, ValueError):
        return None


class Snapshotter:
    def __init__(
        self,
        db: Queryable,
        *,
        langs: Sequence[str],
        interval_s: float,
        stale_after_s: float,
    ):
        self._db = db
        self._langs = list(langs)
        self._interval_s = interval_s
        self._stale_after_s = stale_after_s
        self.latest: Snapshot | None = None

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            started = time.perf_counter()
            try:
                self.latest = await self.build()
                metrics.SNAPSHOTS.labels(result="ok").inc()
                metrics.LAST_SNAPSHOT_TS.set_to_current_time()
            except Exception:  # keep serving the last good snapshot
                metrics.SNAPSHOTS.labels(result="error").inc()
                log.exception("snapshot failed")
            elapsed = time.perf_counter() - started
            metrics.SNAPSHOT_SECONDS.observe(elapsed)
            await sleep_unless_stopped(stop, self._interval_s - elapsed)

    async def build(self) -> Snapshot:
        head = await self._db.query(queries.NEWEST)
        newest_ms = queries.newest_ms(head.rows)
        now = time.time()
        if newest_ms is None:
            payload = assemble(
                now=now, newest=None, langs=self._langs, window_rows=[], top_rows=[],
                minute_rows=[], lag=None, gaps=[], stale_after_s=self._stale_after_s,
            )  # fmt: skip
            return self._pack(payload, None)

        to_ms = newest_ms
        window, top, minutes, lag, gaps = await asyncio.gather(
            self._db.query(
                queries.WINDOW_TOTALS,
                params={"to_ms": to_ms, "window_s": WINDOW_S, "langs": self._langs},
            ),
            self._db.query(
                queries.TOP_ARTICLES,
                params={"to_ms": to_ms, "window_s": WINDOW_S, "per_lang": TOP_N},
            ),
            self._db.query(queries.EDITS_PER_MINUTE, params={"to_ms": to_ms, "minutes": MINUTES}),
            self._db.query(queries.INGEST_LAG, params={"to_ms": to_ms}),
            self._db.query(queries.RECENT_GAPS, params={"to_ms": to_ms, "minutes": MINUTES}),
        )
        payload = assemble(
            now=time.time(),
            newest=to_ms / 1000,
            langs=self._langs,
            window_rows=window.rows,
            top_rows=top.rows,
            minute_rows=minutes.rows,
            lag=lag.rows[0] if lag.rows else None,
            gaps=gaps.rows,
            stale_after_s=self._stale_after_s,
        )
        return self._pack(payload, payload["last_event_age_s"])

    @staticmethod
    def _pack(payload: LivePayload, age: float | None) -> Snapshot:
        metrics.LAST_EVENT_AGE.set(float("nan") if age is None else age)
        body = json.dumps(payload, separators=(",", ":")).encode()
        return Snapshot(body=body, built_at=time.monotonic(), last_event_age_s=age)
