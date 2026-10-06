"""Rebuild the per-minute rollup from the Parquet archive, for whole UTC days.

    python -m livedemos.archive.rebuild --from 2026-10-01 --to 2026-10-03   # [from, to)

For when the rollup is lost or wrong and the raw rows are gone (older than 7 days, or a
host rebuilt from scratch). Deletes the rollup's minutes in the range, then recounts them
from the archive's hour files.

Like `reconcile --repair`, it needs ingest stopped (`make rebuild-rollups` does that), and
it runs as `migrator`, the user that can rewrite tables.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import UTC, date, datetime, timedelta

from livedemos.archive.job import HOUR_S, READ_SCHEMA, Archiver, days_glob, s3_function
from livedemos.clickhouse import ClickHouse
from livedemos.config import ArchiveSettings, archive_settings, clickhouse_settings
from livedemos.logs import setup_logging
from livedemos.reconcile import (
    REBUILD_INSERT_SETTINGS,
    IngestRunning,
    require_ingest_still_stopped,
    require_ingest_stopped,
)

log = logging.getLogger(__name__)

MAX_DAYS = 120  # one glob per run; a longer range is several runs

_RANGE = "minute >= fromUnixTimestamp({from_s:Int64}) AND minute < fromUnixTimestamp({to_s:Int64})"


class ArchiveIncomplete(RuntimeError):
    """The rollup has hours the archive doesn't: rebuilding would lose them."""


async def rebuild(
    ch: ClickHouse,
    settings: ArchiveSettings,
    first: date,
    end: date,
    *,
    quiet: timedelta = timedelta(seconds=30),
) -> int:
    """Rebuild the rollup for days [first, end) from the archive. Returns rollup rows."""
    days = (end - first).days
    if not 0 < days <= MAX_DAYS:
        raise ValueError(f"the range must cover 1 to {MAX_DAYS} days")
    from_s = int(datetime(first.year, first.month, first.day, tzinfo=UTC).timestamp())
    to_s = from_s + days * 24 * HOUR_S
    bounds = {"from_s": from_s, "to_s": to_s}
    hours = list(range(from_s, to_s, HOUR_S))
    archived = await Archiver(settings, ch).existing_hours(hours)
    rolled = await ch.query(
        f"SELECT DISTINCT toUnixTimestamp(toStartOfHour(minute)) AS h "
        f"FROM wiki_edits_per_minute WHERE {_RANGE}",
        params=bounds,
    )
    missing = sorted({int(r["h"]) for r in rolled.rows} - archived)
    if missing:
        first_missing = datetime.fromtimestamp(missing[0], UTC).isoformat()
        raise ArchiveIncomplete(
            f"{len(missing)} hour(s) in the rollup have no archive file, from {first_missing}"
        )
    mark = await require_ingest_stopped(ch, quiet=quiet)
    await ch.execute(
        f"ALTER TABLE wiki_edits_per_minute DELETE WHERE {_RANGE} SETTINGS mutations_sync = 1",
        params=bounds,
    )
    source = s3_function(settings, "Parquet", READ_SCHEMA)
    await ch.execute(
        f"""
        INSERT INTO wiki_edits_per_minute (minute, lang, edits, bot_edits)
        SELECT toStartOfMinute(event_time) AS minute, lang, count(), countIf(is_bot)
        FROM {source}
        WHERE event_time >= fromUnixTimestamp({{from_s:Int64}})
          AND event_time < fromUnixTimestamp({{to_s:Int64}})
        GROUP BY minute, lang
        """,
        params={"url": days_glob(settings.url, hours), **bounds},
        settings={**REBUILD_INSERT_SETTINGS, "use_hive_partitioning": "0"},
    )
    await require_ingest_still_stopped(ch, mark)
    result = await ch.query(
        f"SELECT count() AS n FROM wiki_edits_per_minute WHERE {_RANGE}", params=bounds
    )
    return int(result.rows[0]["n"])


async def _main(args: argparse.Namespace) -> int:
    setup_logging()
    ch = ClickHouse(clickhouse_settings())
    try:
        rows = await rebuild(ch, archive_settings(), args.first, args.end)
    except (IngestRunning, ArchiveIncomplete) as exc:
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
    return asyncio.run(_main(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
