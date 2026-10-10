"""Check the per-minute rollup against the raw rows, and rebuild minutes that disagree.

    livedemos-reconcile            # report mismatched minutes, exit 1 if any
    livedemos-reconcile --repair   # rebuild them from raw rows

A raw insert and its view's insert aren't one transaction, so the rollup can drift. Repair
needs ingest stopped (`make reconcile REPAIR=1` does that), or a late event could count
twice. docs/architecture.md, "ClickHouse".
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from livedemos.config import clickhouse_settings
from livedemos.db.clickhouse import ClickHouse, Queryable
from livedemos.logs import setup_logging
from livedemos.rollup import pages
from livedemos.rollup.lock import LockHeld, exclusive
from livedemos.rollup.maintenance import (
    REBUILD_INSERT_SETTINGS,
    IngestRunning,
    require_ingest_still_stopped,
    require_ingest_stopped,
)

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


@dataclass(frozen=True, slots=True)
class Mismatch:
    minute: datetime
    lang: str
    raw: int
    rolled: int


async def find_mismatches(ch: Queryable, *, now: datetime, settle: timedelta) -> list[Mismatch]:
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


async def repair(
    ch: ClickHouse, mismatches: list[Mismatch], *, quiet: timedelta = timedelta(seconds=30)
) -> int:
    """Rebuild every listed minute (all languages) from raw rows. Returns minutes rebuilt.

    Refuses unless ingest is stopped (`require_ingest_stopped`), and fails if an insert
    ran or landed while it worked (rerun it, with ingest stopped). Holds the maintenance
    lock. The delete and the insert aren't one transaction, but the source is raw rows
    still in ClickHouse: if it's interrupted between them, rerunning restores the minutes.
    """
    minutes = sorted({int(m.minute.timestamp()) for m in mismatches})
    if not minutes:
        return 0
    async with exclusive(ch):
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
        # The page sets for those minutes too, in case the view missed a block. Sets only
        # add, so this can't overcount.
        await pages.add_from_raw(ch, minutes)
        await require_ingest_still_stopped(ch, mark)
    return len(minutes)


async def _main(args: argparse.Namespace) -> int:
    setup_logging()
    settle = timedelta(minutes=args.settle_minutes)
    async with ClickHouse(clickhouse_settings()) as ch:
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
        except (IngestRunning, LockHeld) as exc:
            log.error("not repaired", extra={"reason": str(exc)})
            return 1
        left = await find_mismatches(ch, now=datetime.now(UTC), settle=settle)
        log.info("repaired", extra={"minutes": rebuilt, "still_mismatched": len(left)})
        return 1 if left else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repair", action="store_true", help="rebuild mismatched minutes")
    parser.add_argument("--settle-minutes", type=int, default=15, help="skip this recent window")
    return asyncio.run(_main(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
