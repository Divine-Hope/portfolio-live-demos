"""Rebuild the per-minute rollup from the Parquet archive, for whole UTC days.

    python -m livedemos.archive.rebuild --from 2026-10-01 --to 2026-10-03   # [from, to)

For when the rollup is lost or wrong and the raw rows are gone (older than 7 days, or a
host rebuilt from scratch). The live rollup is never half-rebuilt:

1. Every hour in the range must have a file. An hour without one is either a real outage
   (nothing was ingested) or an hour that never got archived, and the files can't tell
   which. Check `ingest_gaps`, then pass `--allow-missing` to accept them as empty.
2. Count the files into `wiki_edits_per_minute_staging`, one day per query, and check
   every file produced rows, and as many as `archive_hours` recorded for it.
3. With ingest stopped, copy each affected month's other minutes into staging, so it
   holds complete replacement months, then swap each month in with REPLACE PARTITION.
   Each swap is atomic: a month is either all old or all new, never empty.

It holds the maintenance lock (`livedemos.migrate.exclusive`) throughout, so it never
overlaps a migration, a reconcile repair or another rebuild. It needs ingest stopped
(`make rebuild-rollups` does that), and runs as `migrator`.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import UTC, date, datetime, timedelta

from livedemos.archive.job import (
    HOUR_S,
    READ_SCHEMA,
    S3_SETTINGS,
    Archiver,
    days_glob,
    s3_function,
)
from livedemos.clickhouse import ClickHouse
from livedemos.config import ArchiveSettings, archive_settings, clickhouse_settings
from livedemos.logs import setup_logging
from livedemos.maintenance import (
    REBUILD_INSERT_SETTINGS,
    IngestRunning,
    require_ingest_still_stopped,
    require_ingest_stopped,
)
from livedemos.migrate import MigrationError, exclusive

log = logging.getLogger(__name__)

MAX_DAYS = 31
DAY_S = 24 * HOUR_S

_RANGE = "minute >= fromUnixTimestamp({from_s:Int64}) AND minute < fromUnixTimestamp({to_s:Int64})"
_COLUMNS = "minute, lang, edits, bot_edits"


class ArchiveIncomplete(RuntimeError):
    """Hours in the range have no file, or a file produced no rows."""


def _iso(hour_s: int) -> str:
    return datetime.fromtimestamp(hour_s, UTC).isoformat()


async def rebuild(
    ch: ClickHouse,
    settings: ArchiveSettings,
    first: date,
    end: date,
    *,
    allow_missing: bool = False,
    quiet: timedelta = timedelta(seconds=30),
) -> int:
    """Rebuild the rollup for days [first, end) from the archive. Returns rollup rows."""
    days = (end - first).days
    if not 0 < days <= MAX_DAYS:
        raise ValueError(f"the range must cover 1 to {MAX_DAYS} days")
    from_s = int(datetime(first.year, first.month, first.day, tzinfo=UTC).timestamp())
    to_s = from_s + days * DAY_S
    async with exclusive(ch):
        return await _rebuild(ch, settings, from_s, to_s, allow_missing=allow_missing, quiet=quiet)


async def _rebuild(
    ch: ClickHouse,
    settings: ArchiveSettings,
    from_s: int,
    to_s: int,
    *,
    allow_missing: bool,
    quiet: timedelta,
) -> int:
    bounds = {"from_s": from_s, "to_s": to_s}
    hours = list(range(from_s, to_s, HOUR_S))
    archived = await Archiver(settings, ch).existing_hours(hours)
    missing = sorted(set(hours) - archived)
    if missing and not allow_missing:
        raise ArchiveIncomplete(
            f"{len(missing)} hour(s) have no file, first {_iso(missing[0])}. If they're "
            "outages (see ingest_gaps), rerun with --allow-missing to rebuild them as empty."
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
        raise ArchiveIncomplete(f"{len(empty)} file(s) produced no rows, first {_iso(empty[0])}")
    # What each file held when it was written (or found), where this host knows it.
    recorded = await ch.query(
        "SELECT toUnixTimestamp(hour) AS h, argMax(rows, (written_at, rows)) AS n "
        "FROM archive_hours WHERE hour >= fromUnixTimestamp({from_s:Int64}) "
        "AND hour < fromUnixTimestamp({to_s:Int64}) GROUP BY h",
        params=bounds,
    )
    short = sorted(
        int(r["h"]) for r in recorded.rows if staged_edits.get(int(r["h"]), 0) != int(r["n"])
    )
    if short:
        raise ArchiveIncomplete(
            f"{len(short)} file(s) don't hold the rows recorded for them, first {_iso(short[0])}"
        )

    mark = await require_ingest_stopped(ch, quiet=quiet)
    months = sorted({f"{datetime.fromtimestamp(d, UTC):%Y%m}" for d in range(from_s, to_s, DAY_S)})
    # The rest of each month as it is now, so the swap changes only the range.
    await ch.execute(
        f"INSERT INTO wiki_edits_per_minute_staging ({_COLUMNS}) "
        f"SELECT {_COLUMNS} FROM wiki_edits_per_minute "
        "WHERE toYYYYMM(minute) IN {months:Array(UInt32)} "
        "AND NOT (minute >= fromUnixTimestamp({from_s:Int64}) "
        "AND minute < fromUnixTimestamp({to_s:Int64}))",
        params={"months": [int(m) for m in months], **bounds},
        settings=REBUILD_INSERT_SETTINGS,
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
    if rows != sum(staged_hours.values()):
        raise ArchiveIncomplete(
            f"the rollup has {rows} rows in the range after the swap, "
            f"staging had {sum(staged_hours.values())}"
        )
    return rows


async def _main(args: argparse.Namespace) -> int:
    setup_logging()
    ch = ClickHouse(clickhouse_settings())
    try:
        rows = await rebuild(
            ch, archive_settings(), args.first, args.end, allow_missing=args.allow_missing
        )
    except (IngestRunning, ArchiveIncomplete, MigrationError) as exc:
        log.error("not rebuilt", extra={"reason": str(exc)})
        return 1
    finally:
        await ch.aclose()
    log.info("rollup rebuilt from the archive", extra={"rollup_rows": rows})
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--from", dest="first", type=date.fromisoformat, required=True)
    parser.add_argument("--to", dest="end", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--allow-missing", action="store_true", help="rebuild hours with no file as empty"
    )
    return asyncio.run(_main(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
