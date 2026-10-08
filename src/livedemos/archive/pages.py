"""Keep `wiki_pages_per_minute` (migration 0004) complete for its 14 days.

The table holds, per minute and language, the exact set of pages edited. Its materialized
view fills it from every raw insert. Hours without raw rows here (a rebuilt host restores
two days of them; a rollup rebuild works from Parquet) get their sets from the archive.

Sets only add: inserting a page into a minute twice changes nothing. So filling an hour
again is always safe, and an hour counts as filled only once its insert has finished
(`wiki_pages_filled`): an interrupted fill is simply done again.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta

from livedemos.archive.job import HOUR_S, S3_SETTINGS, Archiver, days_glob, s3_function
from livedemos.clickhouse import ClickHouse
from livedemos.config import ArchiveSettings
from livedemos.maintenance import REBUILD_INSERT_SETTINGS

log = logging.getLogger(__name__)

RETENTION = timedelta(days=14)  # the sets' TTL (migration 0004)
_SCHEMA = "event_time DateTime64(3, 'UTC'), lang String, namespace Int32, title String"

_RAW_HOURS = (
    "SELECT DISTINCT toUnixTimestamp(toStartOfHour(event_time)) AS h FROM wiki_edits "
    "WHERE event_time >= fromUnixTimestamp({from_s:Int64})"
)
_FILLED_HOURS = (
    "SELECT DISTINCT toUnixTimestamp(hour) AS h FROM wiki_pages_filled "
    "WHERE hour >= fromUnixTimestamp({from_s:Int64})"
)


def _day_start(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())


async def fill_missing(ch: ClickHouse, settings: ArchiveSettings, *, now: datetime) -> list[int]:
    """Fill every archived hour of the last 14 days that has neither raw rows here (the
    view covered those) nor a finished fill. Returns the hours filled."""
    first_day = (now - RETENTION).date()
    from_s = _day_start(first_day)
    days = (now.date() - first_day).days + 1
    archived = await Archiver(settings, ch).existing_hours(
        list(range(from_s, from_s + days * 24 * HOUR_S, HOUR_S))
    )
    if not archived:
        return []
    raw = {int(r["h"]) for r in (await ch.query(_RAW_HOURS, params={"from_s": from_s})).rows}
    filled = {int(r["h"]) for r in (await ch.query(_FILLED_HOURS, params={"from_s": from_s})).rows}
    missing = sorted(archived - raw - filled)
    if missing:
        await add_from_archive(ch, settings, missing)
        log.info("page sets filled from the archive", extra={"hours": len(missing)})
    return missing


async def add_from_archive(ch: ClickHouse, settings: ArchiveSettings, hours: list[int]) -> None:
    """Add these archived hours' page sets from Parquet, a day per query, then record each
    day's hours as filled."""
    by_day: dict[int, list[int]] = {}
    for hour in hours:
        by_day.setdefault(hour - hour % (24 * HOUR_S), []).append(hour)
    for day_hours in by_day.values():
        await ch.execute(
            f"""
            INSERT INTO wiki_pages_per_minute (minute, lang, pages)
            SELECT toStartOfMinute(event_time) AS minute, lang, uniqExactState(namespace, title)
            FROM {s3_function(settings, "Parquet", _SCHEMA)}
            WHERE toUnixTimestamp(toStartOfHour(event_time)) IN {{hours:Array(UInt32)}}
            GROUP BY minute, lang
            """,
            params={"url": days_glob(settings.url, day_hours), "hours": day_hours},
            settings={**REBUILD_INSERT_SETTINGS, **S3_SETTINGS},
        )
        await ch.execute(
            "INSERT INTO wiki_pages_filled (hour, filled_at) "
            "SELECT fromUnixTimestamp(arrayJoin({hours:Array(UInt32)})), now64(6)",
            params={"hours": day_hours},
        )


async def add_from_raw(ch: ClickHouse, minutes: list[int]) -> None:
    """Add these minutes' page sets from raw rows (a reconcile repair): a set the view
    missed is completed; one it already has is unchanged."""
    await ch.execute(
        """
        INSERT INTO wiki_pages_per_minute (minute, lang, pages)
        SELECT toStartOfMinute(event_time) AS minute, lang, uniqExactState(namespace, title)
        FROM wiki_edits
        WHERE toUnixTimestamp(toStartOfMinute(event_time)) IN {minutes:Array(UInt32)}
        GROUP BY minute, lang
        """,
        params={"minutes": minutes},
        settings=REBUILD_INSERT_SETTINGS,
    )
