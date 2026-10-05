"""Benchmark the API's and ingest's queries at full retained volume (7 days of raw rows).

    make bench                      # 7 days at 11 edits/s (twice the measured live rate)
    python -m livedemos.devtools.bench --days 7 --rate 11 --runs 20

Fills a separate database (`demos_bench`, dropped first) with synthetic edits generated
server-side, then runs each query the snapshot, "Query it" and resume make, with the `api`
profile's limits (2 threads, 200 MB, 3 s) applied as query settings, and reports
ClickHouse's own timing, rows read and peak memory (from system.query_log). It connects
as admin: the `api` user can only read `demos`, and this mustn't touch the real data.
It also measures how fast batches of `flush_max_rows` go in, which bounds how quickly
ingest catches up after an outage.

Rows are bulk-inserted per hour, not one batch a second like ingest, so the parts are
fewer and bigger than a live table's before merges. The query costs are what matters here.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from livedemos.api import queries
from livedemos.api.snapshot import MINUTES, TOP_N, WINDOW_S, Snapshotter
from livedemos.clickhouse import ClickHouse, QueryResult
from livedemos.config import ClickHouseSettings
from livedemos.ingest import resume
from livedemos.ingest.events import Edit, to_row
from livedemos.migrate import migrate

DATABASE = "demos_bench"
LANGS = ["en", "pt", "de"]
# Same as the reader profile in clickhouse/users.d/livedemos.xml.
_DEFAULT_DB = {"database": "default"}
READER_LIMITS = {"max_threads": "2", "max_memory_usage": "200000000", "max_execution_time": "3"}

# One row per event, generated in ClickHouse. Language mix and bot share follow the live
# stream (en 65 %, pt 16 %, de 19 %; about a fifth bots); titles are skewed so a few
# articles are hot and most are edited once.
_FILL = """
INSERT INTO wiki_edits
    (event_id, event_time, ingested_at, wiki, lang, type, namespace, title, is_bot,
     sse_id, ingest_seq)
SELECT
    generateUUIDv4(number) AS event_id,
    t AS event_time,
    t + toIntervalMillisecond(200 + h % 1800) AS ingested_at,
    concat(l, 'wiki') AS wiki,
    l AS lang,
    if(h % 10 = 0, 'new', 'edit') AS type,
    if(h % 10 < 7, 0, toInt32(h % 5)) AS namespace,
    concat('Article ', toString(intDiv(h % 1000003, 1 + bitShiftRight(h, 40) % 64))) AS title,
    h % 5 = 0 AS is_bot,
    concat('[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":',
           toString(toUnixTimestamp64Milli(t)),
           '},{"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":',
           toString(toUnixTimestamp64Milli(t) - 1500), '}]') AS sse_id,
    toUInt64(toUnixTimestamp64Nano(t)) AS ingest_seq
FROM
(
    SELECT
        number,
        cityHash64(number) AS h,
        fromUnixTimestamp64Milli(toInt64({start_ms:Int64} + intDiv(number * 1000, {rate:UInt32})))
            AS t,
        ['en', 'pt', 'de'][1 + (h % 100 >= 65) + (h % 100 >= 81)] AS l
    FROM numbers({first:UInt64}, {count:UInt64})
)
"""


@dataclass(frozen=True, slots=True)
class Case:
    name: str
    sql: str
    params: dict[str, object]


@dataclass(frozen=True, slots=True)
class Result:
    name: str
    p50_ms: float
    p95_ms: float
    rows_read: int
    peak_mb: float


async def fill(ch: ClickHouse, *, days: int, rate: int, now: datetime) -> int:
    """Insert exactly `days` of edits ending at `now`, one INSERT per clock hour (one
    partition each, and small enough for the 900 MiB server cap)."""
    start = now - timedelta(days=days)
    total, at = 0, start
    while at < now:
        slot = at.replace(minute=0, second=0, microsecond=0)
        end = min(slot + timedelta(hours=1), now)
        first = int((at - start).total_seconds() * rate)
        count = int((end - start).total_seconds() * rate) - first
        await ch.execute(
            _FILL,
            params={
                "start_ms": int(start.timestamp() * 1000),
                "rate": rate,
                "first": first,
                "count": count,
            },
            settings={"max_execution_time": "600"},
        )
        total += count
        at = end
    return total


class WithReaderLimits:
    """Every query through it carries the `api` profile's limits as query settings."""

    def __init__(self, ch: ClickHouse):
        self._ch = ch

    async def query(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> QueryResult:
        return await self._ch.query(
            sql, params=params, settings={**READER_LIMITS, **(settings or {})}
        )


def cases(newest_ms: int, last_seq: int) -> list[Case]:
    window = {"to_ms": newest_ms, "langs": LANGS}
    return [
        Case("snapshot: newest event", queries.NEWEST, {}),
        Case("snapshot: 5 min totals", queries.WINDOW_TOTALS, {**window, "window_s": WINDOW_S}),
        Case(
            "snapshot: top articles",
            queries.TOP_ARTICLES,
            {"to_ms": newest_ms, "window_s": WINDOW_S, "per_lang": TOP_N},
        ),
        Case(
            "snapshot: per minute (rollup)",
            queries.EDITS_PER_MINUTE,
            {"to_ms": newest_ms, "minutes": MINUTES},
        ),
        Case("snapshot: ingest lag", queries.INGEST_LAG, {"to_ms": newest_ms}),
        Case(
            "snapshot: recent gaps", queries.RECENT_GAPS, {"to_ms": newest_ms, "minutes": MINUTES}
        ),
        Case("query it: 1 h, all langs", queries.WINDOW_TOTALS, {**window, "window_s": 3_600}),
        Case("query it: 24 h, all langs", queries.WINDOW_TOTALS, {**window, "window_s": 86_400}),
        Case("resume: max ingest_seq", "SELECT max(ingest_seq) AS seq FROM wiki_edits", {}),
        Case(
            "resume: last 20k by ingest order",
            """
            SELECT toString(event_id) AS id, sse_id FROM wiki_edits
            WHERE ingest_seq > {floor:UInt64} ORDER BY ingest_seq DESC LIMIT 20000
            """,
            {"floor": last_seq - resume.SEAM_WINDOW_NS},
        ),
    ]


async def measure(ch: ClickHouse, case: Case, *, runs: int) -> Result:
    timings: list[float] = []
    rows_read = 0
    query_ids = []
    for _ in range(runs):
        query_id = f"bench-{uuid.uuid4()}"
        query_ids.append(query_id)
        result = await WithReaderLimits(ch).query(
            case.sql, params=case.params, settings={"log_queries": "1", "query_id": query_id}
        )
        timings.append(result.stats.elapsed_ms)
        rows_read = result.stats.rows_read
    await ch.execute("SYSTEM FLUSH LOGS")
    peak = await ch.query(
        "SELECT max(memory_usage) AS peak FROM system.query_log "
        "WHERE query_id IN {ids:Array(String)} AND type = 'QueryFinish'",
        params={"ids": query_ids},
    )
    return Result(
        name=case.name,
        p50_ms=statistics.median(timings),
        p95_ms=_p95(timings),
        rows_read=rows_read,
        peak_mb=int(peak.rows[0]["peak"]) / 1e6,
    )


async def snapshot_build(ch: ClickHouse, *, runs: int) -> tuple[float, float]:
    snapshotter = Snapshotter(WithReaderLimits(ch), langs=LANGS, interval_s=1, stale_after_s=60)
    timings = []
    for _ in range(runs):
        started = time.perf_counter()
        await snapshotter.build()
        timings.append((time.perf_counter() - started) * 1000)
    return statistics.median(timings), _p95(timings)


async def insert_throughput(ch: ClickHouse, *, batch_rows: int, batches: int) -> float:
    """Rows per second through ingest's own write path: JSONEachRow over HTTP, one token
    per batch, through the rollup view. Bounds how fast ingest can catch up."""
    now = datetime.now(UTC)
    elapsed = 0.0
    for i in range(batches):
        rows = [
            to_row(
                Edit(
                    event_id=str(uuid.uuid4()),
                    event_time=now,
                    wiki="enwiki",
                    lang="en",
                    type="edit",
                    namespace=0,
                    title=f"Catch-up {n % 500}",
                    is_bot=n % 5 == 0,
                ),
                sse_id='[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":0}]',
                ingest_seq=time.time_ns() + n,
                ingested_at=now,
            )
            for n in range(batch_rows)
        ]
        started = time.perf_counter()  # the insert, not building the rows
        await ch.insert("wiki_edits", rows, dedup_token=f"bench-{i}-{uuid.uuid4()}")
        elapsed += time.perf_counter() - started
    return batch_rows * batches / elapsed


async def storage(ch: ClickHouse) -> list[dict[str, Any]]:
    parts = await ch.query(
        """
        SELECT table, count() AS parts, sum(rows) AS rows,
               sum(data_compressed_bytes) AS compressed, sum(data_uncompressed_bytes) AS raw
        FROM system.parts WHERE database = currentDatabase() AND active
        GROUP BY table ORDER BY table
        """
    )
    return parts.rows


def _p95(values: list[float]) -> float:
    return statistics.quantiles(values, n=20)[-1] if len(values) >= 2 else values[0]


async def run(settings: ClickHouseSettings, *, days: int, rate: int, runs: int) -> None:
    ch = ClickHouse(settings.model_copy(update={"database": DATABASE, "timeout_s": 600}))
    try:
        await ch.execute(f"DROP DATABASE IF EXISTS {DATABASE} SYNC", settings=_DEFAULT_DB)
        await migrate(ch)
        started = time.perf_counter()
        total = await fill(ch, days=days, rate=rate, now=datetime.now(UTC))
        took = time.perf_counter() - started
        print(f"filled {total:,} rows ({days} days at {rate}/s) in {took:.0f} s")

        newest = queries.newest_ms((await ch.query(queries.NEWEST)).rows)
        if newest is None:
            raise SystemExit("no rows were generated")
        head = await ch.query("SELECT max(ingest_seq) AS seq FROM wiki_edits")
        last_seq = int(head.rows[0]["seq"])

        print("\n| Query | p50 ms | p95 ms | Rows read | Peak memory |")
        print("|---|---:|---:|---:|---:|")
        for case in cases(newest, last_seq):
            r = await measure(ch, case, runs=runs)
            cells = [r.name, f"{r.p50_ms:.1f}", f"{r.p95_ms:.1f}", f"{r.rows_read:,}"]
            print("| " + " | ".join([*cells, f"{r.peak_mb:.1f} MB"]) + " |")

        p50, p95 = await snapshot_build(ch, runs=runs)
        print(f"\nwhole snapshot build (5 queries at once): p50 {p50:.0f} ms, p95 {p95:.0f} ms")
        rows_per_s = await insert_throughput(ch, batch_rows=5_000, batches=40)
        print(f"insert throughput, 5,000-row JSON batches via the view: {rows_per_s:,.0f} rows/s")

        print("\n| Table | Parts | Rows | Compressed | Uncompressed |")
        print("|---|---:|---:|---:|---:|")
        for row in await storage(ch):
            print(
                f"| {row['table']} | {row['parts']} | {int(row['rows']):,} | "
                f"{int(row['compressed']) / 1e6:.0f} MB | {int(row['raw']) / 1e6:.0f} MB |"
            )
    finally:
        await ch.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--rate", type=int, default=11, help="edits per second to simulate")
    parser.add_argument("--runs", type=int, default=20)
    args = parser.parse_args()
    asyncio.run(run(ClickHouseSettings(), days=args.days, rate=args.rate, runs=args.runs))


if __name__ == "__main__":
    main()
