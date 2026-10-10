"""Keep `wiki_pages_per_minute` (migration 0004) complete for its 14 days.

The table holds, per minute and language, the exact set of pages edited. Its materialized
view fills it from every raw insert. Every hour is then completed once it's final: by the
archive service from raw rows when it writes the hour (archive/job.py), or from the hour's
Parquet file here (a rebuilt host's older days, a rollup rebuild).

Sets only add: inserting a page into a minute twice changes nothing. So filling an hour
again is always safe, and an hour counts as filled only once its insert has finished
(`wiki_pages_filled`): an interrupted fill is simply done again.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime, timedelta

from livedemos.archive.s3 import (
    HOUR_S,
    S3_SETTINGS,
    days_glob,
    existing_hours,
    hour_url,
    s3_function,
)
from livedemos.config import ArchiveSettings
from livedemos.dates import day_start
from livedemos.db.clickhouse import Warehouse
from livedemos.rollup.maintenance import REBUILD_INSERT_SETTINGS

log = logging.getLogger(__name__)

RETENTION = timedelta(days=14)  # the sets' TTL (migration 0004)
SCHEMA = "event_time DateTime64(3, 'UTC'), lang String, namespace Int32, title String"

# Every page set comes from this, whatever the source: raw rows or Parquet.
_ADD = """
INSERT INTO wiki_pages_per_minute (minute, lang, pages)
SELECT toStartOfMinute(event_time) AS minute, lang, uniqExactState(namespace, title)
FROM {source}
WHERE {where}
GROUP BY minute, lang
"""
_MARK_FILLED = (
    "INSERT INTO wiki_pages_filled (hour, filled_at) "
    "SELECT fromUnixTimestamp(arrayJoin({hours:Array(UInt32)})), now64(6)"
)
_FILLED = (
    "SELECT DISTINCT toUnixTimestamp(hour) AS h FROM wiki_pages_filled "
    "WHERE toUnixTimestamp(hour) IN {hours:Array(UInt32)}"
)


async def _add(
    ch: Warehouse,
    *,
    source: str,
    where: str,
    params: Mapping[str, object],
    settings: Mapping[str, str] | None = None,
) -> None:
    await ch.execute(_ADD.format(source=source, where=where), params=params, settings=settings)


async def filled(ch: Warehouse, hours: list[int]) -> set[int]:
    """Which of these hours have a finished fill."""
    result = await ch.query(_FILLED, params={"hours": hours})
    return {int(r["h"]) for r in result.rows}


async def mark_filled(ch: Warehouse, hours: list[int]) -> None:
    await ch.execute(_MARK_FILLED, params={"hours": hours})


async def fill_missing(ch: Warehouse, settings: ArchiveSettings, *, now: datetime) -> list[int]:
    """Fill every archived hour of the last 14 days without a finished fill. Returns the
    hours filled. The archive service records each hour it writes (job.py); this catches
    the rest: a rebuilt host's older days, and a host from before the table."""
    first_day = (now - RETENTION).date()
    from_s = day_start(first_day)
    days = (now.date() - first_day).days + 1
    hours = list(range(from_s, from_s + days * 24 * HOUR_S, HOUR_S))
    archived = await existing_hours(ch, settings, hours)
    if not archived:
        return []
    missing = sorted(archived - await filled(ch, sorted(archived)))
    if missing:
        await add_from_archive(ch, settings, missing)
        log.info("page sets filled from the archive", extra={"hours": len(missing)})
    return missing


async def add_from_archive(ch: Warehouse, settings: ArchiveSettings, hours: list[int]) -> None:
    """Add these archived hours' page sets from Parquet, a day per query, then record each
    day's hours as filled."""
    by_day: dict[int, list[int]] = {}
    for hour in hours:
        by_day.setdefault(hour - hour % (24 * HOUR_S), []).append(hour)
    for day_hours in by_day.values():
        await _add(
            ch,
            source=s3_function(settings, "Parquet", SCHEMA),
            where="toUnixTimestamp(toStartOfHour(event_time)) IN {hours:Array(UInt32)}",
            params={"url": days_glob(settings.url, day_hours), "hours": day_hours},
            settings={**REBUILD_INSERT_SETTINGS, **S3_SETTINGS},
        )
        await mark_filled(ch, day_hours)


async def add_from_file(ch: Warehouse, settings: ArchiveSettings, hour_s: int) -> None:
    """Add one archived hour's page sets from its file, and record it as filled."""
    await _add(
        ch,
        source=s3_function(settings, "Parquet", SCHEMA),
        where="1",
        params={"url": hour_url(settings.url, hour_s)},
        settings=S3_SETTINGS,
    )
    await mark_filled(ch, [hour_s])


async def add_from_raw_hour(ch: Warehouse, hour_s: int) -> None:
    """Complete an hour's page sets from raw rows once it's final, and record it as filled.
    The view fills them on every insert, but a view's block can be lost on its own (a
    crash between tables); this checks every hour once. Sets only add."""
    await _add(
        ch,
        source="wiki_edits",
        where=(
            "event_time >= fromUnixTimestamp({from_s:Int64}) "
            "AND event_time < fromUnixTimestamp({to_s:Int64})"
        ),
        params={"from_s": hour_s, "to_s": hour_s + HOUR_S},
    )
    await mark_filled(ch, [hour_s])


async def add_from_raw(ch: Warehouse, minutes: list[int]) -> None:
    """Add these minutes' page sets from raw rows (a reconcile repair): a set the view
    missed is completed; one it already has is unchanged."""
    await _add(
        ch,
        source="wiki_edits",
        where="toUnixTimestamp(toStartOfMinute(event_time)) IN {minutes:Array(UInt32)}",
        params={"minutes": minutes},
        settings=REBUILD_INSERT_SETTINGS,
    )
