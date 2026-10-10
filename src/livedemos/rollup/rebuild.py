"""Rebuild the per-minute rollup from the Parquet archive, for whole UTC days.

    livedemos-rebuild --from 2026-10-01 --to 2026-10-03   # [from, to)

For when the rollup is lost or wrong and the raw rows are gone (older than 7 days, or a
host rebuilt from scratch). The live rollup is never half-rebuilt:

1. Every hour in the range must have a file. An hour without one is either a real outage
   (nothing was ingested) or an hour that never got archived, and the files can't tell
   which. Check `ingest_gaps`, then pass `--allow-missing` to rebuild the hours that have
   files and leave the others as they are in the live rollup.
2. Count the files into `wiki_edits_per_minute_staging`, one day per query, and check
   every file produced rows, and as many as `archive_hours` recorded for it.
3. With ingest stopped, copy each affected month's other minutes into staging, so it
   holds complete replacement months, then swap each month in with REPLACE PARTITION.
   Each swap is atomic: a month is either all old or all new, never empty.

It holds the maintenance lock (`rollup/lock.py`) throughout, so it never
overlaps a migration, a reconcile repair or another rebuild. It needs ingest stopped
(`make rebuild-rollups` does that), and runs as `migrator`.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from datetime import UTC, date, datetime, timedelta

from livedemos.archive.job import READ_SCHEMA, RECORDED_COUNTS, Archiver
from livedemos.archive.s3 import HOUR_S, S3_SETTINGS, days_glob, s3_function
from livedemos.config import ArchiveSettings, archive_settings, clickhouse_settings
from livedemos.dates import day_start, iso
from livedemos.db.clickhouse import ClickHouse
from livedemos.logs import setup_logging
from livedemos.rollup import pages
from livedemos.rollup.lock import LockHeld, exclusive
from livedemos.rollup.maintenance import (
    REBUILD_INSERT_SETTINGS,
    IngestMark,
    IngestRunning,
    require_ingest_still_stopped,
    require_ingest_stopped,
)

log = logging.getLogger(__name__)

MAX_DAYS = 31
DAY_S = 24 * HOUR_S

_RANGE = "minute >= fromUnixTimestamp({from_s:Int64}) AND minute < fromUnixTimestamp({to_s:Int64})"
_COLUMNS = "minute, lang, edits, bot_edits"


class ArchiveIncomplete(RuntimeError):
    """Hours in the range have no file, or a file produced no rows."""


async def rebuild(
    ch: ClickHouse,
    settings: ArchiveSettings,
    first: date,
    end: date,
    *,
    allow_missing: bool = False,
    quiet: timedelta = timedelta(seconds=30),
    mark: IngestMark | None = None,
) -> int:
    """Rebuild the rollup for days [first, end) from the archive. Returns rollup rows.

    `mark`: where ingest was before the caller started; checked before anything is
    replaced, so ingest writing at any point since makes it refuse.
    """
    days = (end - first).days
    if not 0 < days <= MAX_DAYS:
        raise ValueError(f"the range must cover 1 to {MAX_DAYS} days")
    from_s = day_start(first)
    to_s = from_s + days * DAY_S
    async with exclusive(ch):
        return await _rebuild(
            ch, settings, from_s, to_s, allow_missing=allow_missing, quiet=quiet, mark=mark
        )


async def _rebuild(
    ch: ClickHouse,
    settings: ArchiveSettings,
    from_s: int,
    to_s: int,
    *,
    allow_missing: bool,
    quiet: timedelta,
    mark: IngestMark | None,
) -> int:
    bounds = {"from_s": from_s, "to_s": to_s}
    hours = list(range(from_s, to_s, HOUR_S))
    archived = await Archiver(settings, ch).existing_hours(hours)
    missing = sorted(set(hours) - archived)
    if missing and not allow_missing:
        raise ArchiveIncomplete(
            f"{len(missing)} hour(s) have no file, first {iso(missing[0])}. If they're "
            "outages (see ingest_gaps), rerun with --allow-missing to rebuild the rest."
        )

    await ch.execute("TRUNCATE TABLE wiki_edits_per_minute_staging")
    source = s3_function(settings, "Parquet", READ_SCHEMA)
    for day_s in range(from_s, to_s, DAY_S):
        day_hours = [h for h in range(day_s, day_s + DAY_S, HOUR_S) if h in archived]
        if not day_hours:
            continue
        await ch.execute(
            f"""
            INSERT INTO wiki_edits_per_minute_staging ({_COLUMNS})
            SELECT toStartOfMinute(event_time) AS minute, lang, count(), countIf(is_bot)
            FROM {source}
            WHERE event_time >= fromUnixTimestamp({{from_s:Int64}})
              AND event_time < fromUnixTimestamp({{to_s:Int64}})
            GROUP BY minute, lang
            """,
            params={
                "url": days_glob(settings.url, day_hours),
                "from_s": day_s,
                "to_s": day_s + DAY_S,
            },
            settings={**REBUILD_INSERT_SETTINGS, **S3_SETTINGS},
        )
    staged = await ch.query(
        "SELECT toUnixTimestamp(toStartOfHour(minute)) AS h, count() AS n, sum(edits) AS e "
        f"FROM wiki_edits_per_minute_staging WHERE {_RANGE} GROUP BY h",
        params=bounds,
    )
    staged_hours = {int(r["h"]): int(r["n"]) for r in staged.rows}
    staged_edits = {int(r["h"]): int(r["e"]) for r in staged.rows}
    empty = sorted(archived - set(staged_hours))
    if empty:  # a file is only written for an hour with rows
        raise ArchiveIncomplete(f"{len(empty)} file(s) produced no rows, first {iso(empty[0])}")
    # What each file held when it was written (or found), where this host knows it.
    recorded = await ch.query(RECORDED_COUNTS, params=bounds)
    short = sorted(
        int(r["h"]) for r in recorded.rows if staged_edits.get(int(r["h"]), 0) != int(r["n"])
    )
    if short:
        raise ArchiveIncomplete(
            f"{len(short)} file(s) don't hold the rows recorded for them, first {iso(short[0])}"
        )

    if mark is None:
        mark = await require_ingest_stopped(ch, quiet=quiet)
    else:
        await require_ingest_still_stopped(ch, mark)
    months = sorted({f"{datetime.fromtimestamp(d, UTC):%Y%m}" for d in range(from_s, to_s, DAY_S)})
    # Everything else in those months as it is now, including hours with no file, so the
    # swap changes only the hours rebuilt from the archive.
    await ch.execute(
        f"INSERT INTO wiki_edits_per_minute_staging ({_COLUMNS}) "
        f"SELECT {_COLUMNS} FROM wiki_edits_per_minute "
        "WHERE toYYYYMM(minute) IN {months:Array(UInt32)} "
        "AND toUnixTimestamp(toStartOfHour(minute)) NOT IN {rebuilt:Array(UInt32)}",
        params={"months": [int(m) for m in months], "rebuilt": sorted(archived)},
        settings=REBUILD_INSERT_SETTINGS,
    )
    expected = await ch.query(
        f"SELECT count() AS n FROM wiki_edits_per_minute_staging WHERE {_RANGE}", params=bounds
    )
    for month in months:  # a computed integer, not input
        await ch.execute(
            f"ALTER TABLE wiki_edits_per_minute REPLACE PARTITION ID '{month}' "
            "FROM wiki_edits_per_minute_staging"
        )
    await require_ingest_still_stopped(ch, mark)
    await ch.execute("TRUNCATE TABLE wiki_edits_per_minute_staging")
    result = await ch.query(
        f"SELECT count() AS n FROM wiki_edits_per_minute WHERE {_RANGE}", params=bounds
    )
    rows = int(result.rows[0]["n"])
    # The page sets for the same hours, where they're still kept (sets only add).
    recent = [h for h in sorted(archived) if h >= time.time() - pages.RETENTION.total_seconds()]
    if recent:
        await pages.add_from_archive(ch, settings, recent)
    if rows != int(expected.rows[0]["n"]):
        raise ArchiveIncomplete(
            f"the rollup has {rows} rows in the range after the swap, "
            f"staging had {expected.rows[0]['n']}"
        )
    return rows


async def _main(args: argparse.Namespace) -> int:
    setup_logging()
    async with ClickHouse(clickhouse_settings()) as ch:
        try:
            rows = await rebuild(
                ch, archive_settings(), args.first, args.end, allow_missing=args.allow_missing
            )
        except (IngestRunning, ArchiveIncomplete, LockHeld) as exc:
            log.error("not rebuilt", extra={"reason": str(exc)})  # noqa: TRY400 (a refusal: the reason is the whole story)
            return 1
    log.info("rollup rebuilt from the archive", extra={"rollup_rows": rows})
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--from", dest="first", type=date.fromisoformat, required=True)
    parser.add_argument("--to", dest="end", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="rebuild the hours that have files; leave the others as they are",
    )
    return asyncio.run(_main(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
