"""Check the per-minute rollup against the raw rows, and rebuild minutes that disagree.

    python -m livedemos.reconcile            # report mismatched minutes, exit 1 if any
    python -m livedemos.reconcile --repair   # rebuild them from raw rows

Why this exists: a ClickHouse INSERT writes the raw part and then the materialized view's
part, and the two are not one transaction. If the second write fails, the retry's
deduplication can't always tell which half landed, so the rollup can drift from raw. Raw
rows are the source of truth for as long as they're kept (7 days); this compares every
(minute, language) in that range and, with --repair, rebuilds the ones that differ.

Minutes newer than `--settle-minutes` are skipped: late events are still arriving there.

Repair deletes a minute's rollup rows and rebuilds them from raw rows. If ingest wrote a
late event for that minute in between, the view would add it and the rebuild would count
it again. So repair needs ingest stopped (`make reconcile REPAIR=1` stops and restarts it):
it refuses while an ingest insert is running or rows are still arriving, and checks again
afterwards that none ran or arrived during the repair. It runs as `migrator`, not as ingest.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from livedemos.clickhouse import ClickHouse
from livedemos.config import clickhouse_settings
from livedemos.ingest.resume import INSERT_QUERY_ID_PREFIX
from livedemos.logs import setup_logging

log = logging.getLogger(__name__)

_MISMATCHES = """
SELECT toUnixTimestamp(minute) AS minute_s, lang, raw, rolled
FROM (
    SELECT toStartOfMinute(event_time) AS minute, lang, count() AS raw
    FROM wiki_edits
    WHERE event_time >= fromUnixTimestamp({from_s:Int64})
      AND event_time < fromUnixTimestamp({to_s:Int64})
    GROUP BY minute, lang
) AS r
FULL OUTER JOIN (
    SELECT minute, lang, sum(edits) AS rolled
    FROM wiki_edits_per_minute
    WHERE minute >= fromUnixTimestamp({from_s:Int64})
      AND minute < fromUnixTimestamp({to_s:Int64})
    GROUP BY minute, lang
) AS m USING (minute, lang)
WHERE raw != rolled
ORDER BY minute, lang
"""

# Raw TTL drops whole days, so the oldest raw partition is complete: start there.
_RAW_RANGE = """
SELECT toUnixTimestamp(min(event_time)) AS oldest_s, count() AS n FROM wiki_edits
"""


_LAST_INGEST = "SELECT toUnixTimestamp64Milli(max(ingested_at)) AS ms, count() AS n FROM wiki_edits"
# max(ingest_seq) comes from the seq_max projection: a cheap sentinel for "a batch landed".
_LAST_SEQ = "SELECT max(ingest_seq) AS seq FROM wiki_edits"
# An insert whose raw rows are already visible can still be writing the rollup (the view
# runs after the raw part commits), so "no new rows" isn't enough: no insert may be running.
_RUNNING_INSERTS = (
    "SELECT count() AS n FROM system.processes "
    "WHERE startsWith(query_id, {prefix:String}) AND current_database = currentDatabase()"
)


class IngestRunning(RuntimeError):
    """Rows arrived within the quiet period: repairing now could double-count."""


@dataclass(frozen=True, slots=True)
class Mismatch:
    minute: datetime
    lang: str
    raw: int
    rolled: int


async def find_mismatches(ch: ClickHouse, *, now: datetime, settle: timedelta) -> list[Mismatch]:
    head = (await ch.query(_RAW_RANGE)).rows[0]
    if not int(head["n"]):
        return []
    # Whole minutes only: the first raw minute may be cut by the TTL boundary.
    from_s = (int(head["oldest_s"]) // 60 + 1) * 60
    to_s = int((now - settle).timestamp()) // 60 * 60
    if to_s <= from_s:
        return []
    result = await ch.query(_MISMATCHES, params={"from_s": from_s, "to_s": to_s})
    return [
        Mismatch(
            minute=datetime.fromtimestamp(int(r["minute_s"]), UTC),
            lang=str(r["lang"]),
            raw=int(r["raw"]),
            rolled=int(r["rolled"]),
        )
        for r in result.rows
    ]


async def _last_ingest_ms(ch: ClickHouse) -> int:
    row = (await ch.query(_LAST_INGEST)).rows[0]
    return int(row["ms"]) if int(row["n"]) else 0


async def _ingest_state(ch: ClickHouse) -> tuple[int, int]:
    """(inserts running now, highest committed ingest_seq)."""
    running = await ch.query(_RUNNING_INSERTS, params={"prefix": INSERT_QUERY_ID_PREFIX})
    seq = await ch.query(_LAST_SEQ)
    return int(running.rows[0]["n"]), int(seq.rows[0]["seq"]) if seq.rows else 0


@dataclass(frozen=True, slots=True)
class IngestMark:
    """Where ingest was when a rollup rewrite started: highest seq, last ingested_at."""

    seq: int
    last_ms: int


async def require_ingest_stopped(
    ch: ClickHouse, *, quiet: timedelta = timedelta(seconds=30)
) -> IngestMark:
    """Raise IngestRunning unless no ingest insert runs and nothing landed for `quiet`.

    Rewriting rollup minutes while ingest writes could count a late event twice: once
    through the view, once in the rebuild. Pass the mark to `require_ingest_still_stopped`
    when done.
    """
    running, seq = await _ingest_state(ch)
    if running:
        raise IngestRunning("an ingest insert is still running; stop ingest first")
    last_ms = await _last_ingest_ms(ch)
    if last_ms and datetime.now(UTC).timestamp() * 1000 - last_ms < quiet.total_seconds() * 1000:
        raise IngestRunning("rows are still arriving; stop ingest first")
    return IngestMark(seq=seq, last_ms=last_ms)


async def require_ingest_still_stopped(ch: ClickHouse, mark: IngestMark) -> None:
    running, seq = await _ingest_state(ch)
    if running or seq != mark.seq or await _last_ingest_ms(ch) != mark.last_ms:
        raise IngestRunning("ingest wrote during the rewrite; stop it and run again")


# The rollup keeps hashes of recent inserts to drop retried ones. A rebuild can produce a
# block identical to an earlier one (the view's original block for those minutes, or a
# previous rebuild's), which dedup would drop silently, after the DELETE. 26.8 doesn't
# dedup INSERT ... SELECT by default, but `deduplicate_insert_select` can turn it on, and
# then it wins over `insert_deduplicate`. Rebuilds are deliberate: turn both off.
REBUILD_INSERT_SETTINGS = {"insert_deduplicate": "0", "deduplicate_insert_select": "disable"}


async def repair(
    ch: ClickHouse, mismatches: list[Mismatch], *, quiet: timedelta = timedelta(seconds=30)
) -> int:
    """Rebuild every listed minute (all languages) from raw rows. Returns minutes rebuilt.

    Refuses unless ingest is stopped (`require_ingest_stopped`), and fails if an insert
    ran or landed while it worked (rerun it, with ingest stopped).
    """
    minutes = sorted({int(m.minute.timestamp()) for m in mismatches})
    if not minutes:
        return 0
    mark = await require_ingest_stopped(ch, quiet=quiet)
    # Integers, not timestamps: ClickHouse binds them as an Array(UInt32) and compares
    # toUnixTimestamp(minute), so no time zone or format can creep in.
    await ch.execute(
        "ALTER TABLE wiki_edits_per_minute DELETE "
        "WHERE toUnixTimestamp(minute) IN {minutes:Array(UInt32)} "
        "SETTINGS mutations_sync = 1",
        params={"minutes": minutes},
    )
    await ch.execute(
        """
        INSERT INTO wiki_edits_per_minute (minute, lang, edits, bot_edits)
        SELECT toStartOfMinute(event_time) AS minute, lang, count(), countIf(is_bot)
        FROM wiki_edits
        WHERE toUnixTimestamp(toStartOfMinute(event_time)) IN {minutes:Array(UInt32)}
        GROUP BY minute, lang
        """,
        params={"minutes": minutes},
        settings=REBUILD_INSERT_SETTINGS,
    )
    await require_ingest_still_stopped(ch, mark)
    return len(minutes)


async def _main(args: argparse.Namespace) -> int:
    setup_logging()
    ch = ClickHouse(clickhouse_settings())
    try:
        settle = timedelta(minutes=args.settle_minutes)
        found = await find_mismatches(ch, now=datetime.now(UTC), settle=settle)
        for m in found:
            log.warning(
                "rollup disagrees with raw",
                extra={
                    "minute": m.minute.isoformat(),
                    "lang": m.lang,
                    "raw": m.raw,
                    "rolled": m.rolled,
                },
            )
        if not found:
            log.info("rollup matches raw")
            return 0
        if not args.repair:
            return 1
        try:
            rebuilt = await repair(ch, found)
        except IngestRunning as exc:
            log.error("not repaired", extra={"reason": str(exc)})
            return 1
        left = await find_mismatches(ch, now=datetime.now(UTC), settle=settle)
        log.info("repaired", extra={"minutes": rebuilt, "still_mismatched": len(left)})
        return 1 if left else 0
    finally:
        await ch.aclose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repair", action="store_true", help="rebuild mismatched minutes")
    parser.add_argument("--settle-minutes", type=int, default=15, help="skip this recent window")
    return asyncio.run(_main(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
