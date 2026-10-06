"""Bring a host that has lost its data back from the archive, before ingest starts.

`python -m livedemos.migrate` runs this after the migrations, so on every boot and deploy.
It does nothing unless the raw table is empty: a host rebuilt from scratch, or one that was
down longer than raw retention. Then, for every archived hour newer than the rollup's last
minute (up to 90 days back):

- the newest archived day and the one before go back into the raw table, whole, with the
  ids and ingest times they were archived with. The rollup gets them through its view, as
  it did the first time, and the archive service finds raw rows that match its files;
- older days are rebuilt straight into the rollup (`rebuild --allow-missing`, a month at
  a time).

Ingest then replays the stream from a little before the newest archived event and skips
the ids it restored (see ingest/resume.py): the seam is matched by event id, not by time,
so a late event is neither lost nor counted twice.

A failure stops `migrate`, so the stack doesn't start with history it could have had. Fix
the cause, or start without the restore: `ARCHIVE_RESTORE=false` (docs/runbook.md).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

from livedemos.archive.job import HOUR_S, S3_SETTINGS, Archiver, days_glob, s3_function
from livedemos.archive.rebuild import rebuild
from livedemos.clickhouse import ClickHouse
from livedemos.config import ArchiveSettings
from livedemos.maintenance import (
    REBUILD_INSERT_SETTINGS,
    require_ingest_still_stopped,
    require_ingest_stopped,
)

log = logging.getLogger(__name__)

ROLLUP_RETENTION = timedelta(days=90)  # the rollup's TTL (migration 0001)
RAW_RETENTION = timedelta(days=7)  # the raw table's TTL
RAW_DAYS = 2  # the newest archived day and the one before go back into the raw table

# Both answered from part metadata.
_RAW_ROWS = "SELECT count() AS n FROM wiki_edits"
_ROLLUP_END = (
    "SELECT toUnixTimestamp(max(minute)) AS newest_s, count() AS n FROM wiki_edits_per_minute"
)
# Any row ingest wrote itself (restored rows have no bookmark). Stops at the first.
_INGESTED = "SELECT 1 FROM wiki_edits WHERE sse_id != '' LIMIT 1"
_RESTORE_SCHEMA = (
    "event_id String, event_time DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), "
    "wiki String, lang String, type String, namespace Int32, title String, is_bot Bool"
)


@dataclass(frozen=True, slots=True)
class Restored:
    rollup_hours: list[int] = field(default_factory=list)  # rebuilt into the rollup
    raw_hours: list[int] = field(default_factory=list)  # back in the raw table
    raw_rows: int = 0


def _day_start(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())


def _day(seconds: int) -> date:
    return datetime.fromtimestamp(seconds, UTC).date()


def _months(first: date, end: date) -> list[tuple[date, date]]:
    """[first, end) split at month boundaries: each piece fits one rebuild."""
    out = []
    while first < end:
        next_month = date(first.year + first.month // 12, first.month % 12 + 1, 1)
        out.append((first, min(next_month, end)))
        first = next_month
    return out


async def restore(ch: ClickHouse, settings: ArchiveSettings, *, now: datetime) -> Restored:
    """Restore archived hours the rollup doesn't have, if raw rows are gone."""
    if int((await ch.query(_RAW_ROWS)).rows[0]["n"]):
        if (await ch.query(_INGESTED)).rows:
            return Restored()  # ingest resumes from its bookmark; nothing was lost
        return await _finish_raw(ch, settings, now=now)
    end = (await ch.query(_ROLLUP_END)).rows[0]
    rollup_end = int(end["newest_s"]) if int(end["n"]) else None

    first_day = (now - ROLLUP_RETENTION).date()
    if rollup_end is not None:
        first_day = max(first_day, _day(rollup_end))
    archived = await _archived(ch, settings, first_day, now)
    missing = sorted(h for h in archived if rollup_end is None or h > rollup_end)
    if not missing:
        return Restored()

    raw_from_s = _day_start(_raw_from(missing[-1], now))
    to_rollup = [h for h in missing if h < raw_from_s]
    to_raw = [h for h in missing if h >= raw_from_s]
    log.info(
        "restoring from the archive",
        extra={
            "rollup_hours": len(to_rollup),
            "raw_hours": len(to_raw),
            "from": datetime.fromtimestamp(missing[0], UTC).isoformat(),
            "to": datetime.fromtimestamp(missing[-1] + HOUR_S, UTC).isoformat(),
        },
    )
    # Ingest waits for `migrate`, so it can't be running; make sure, and that it stays so.
    mark = await require_ingest_stopped(ch, quiet=timedelta(0))
    if to_rollup:
        last = min(_day(to_rollup[-1]) + timedelta(days=1), _day(raw_from_s))
        for start, stop in _months(_day(to_rollup[0]), last):
            # Allow missing hours: an outage has no file, and nothing to restore.
            await rebuild(ch, settings, start, stop, allow_missing=True, mark=mark)
    await require_ingest_still_stopped(ch, mark)
    raw_rows = await _restore_raw(ch, settings, to_raw) if to_raw else 0
    return Restored(rollup_hours=to_rollup, raw_hours=to_raw, raw_rows=raw_rows)


def _raw_from(newest_hour: int, now: datetime) -> date:
    """The first day restored into raw: whole days, and none the raw TTL drops at once."""
    return max(
        _day(newest_hour) - timedelta(days=RAW_DAYS - 1),
        (now - RAW_RETENTION).date() + timedelta(days=1),
    )


async def _archived(
    ch: ClickHouse, settings: ArchiveSettings, first_day: date, now: datetime
) -> set[int]:
    days = (now.date() - first_day).days + 1
    start = _day_start(first_day)
    return await Archiver(settings, ch).existing_hours(
        list(range(start, start + days * 24 * HOUR_S, HOUR_S))
    )


async def _finish_raw(ch: ClickHouse, settings: ArchiveSettings, *, now: datetime) -> Restored:
    """Raw holds only restored rows: a restore finished, or stopped part way through.

    An `INSERT ... SELECT` over many files isn't atomic. If the rows don't match the files,
    take the restored days out of raw and the rollup, and put them back.
    """
    archived = await _archived(ch, settings, (now - ROLLUP_RETENTION).date(), now)
    if not archived:
        return Restored()
    raw_from_s = _day_start(_raw_from(max(archived), now))
    hours = sorted(h for h in archived if h >= raw_from_s)
    expected = await _file_rows(ch, settings, hours)
    rows = int((await ch.query(_RAW_ROWS)).rows[0]["n"])
    if rows == expected:
        return Restored()
    log.warning(
        "an earlier restore stopped part way; restoring raw rows again",
        extra={"raw_rows": rows, "in_files": expected},
    )
    await require_ingest_stopped(ch, quiet=timedelta(0))
    for table, column in (("wiki_edits", "event_time"), ("wiki_edits_per_minute", "minute")):
        await ch.execute(
            f"ALTER TABLE {table} DELETE WHERE {column} >= fromUnixTimestamp({{from_s:Int64}}) "
            "SETTINGS mutations_sync = 1",
            params={"from_s": raw_from_s},
        )
    return Restored(raw_hours=hours, raw_rows=await _restore_raw(ch, settings, hours))


async def _file_rows(ch: ClickHouse, settings: ArchiveSettings, hours: list[int]) -> int:
    result = await ch.query(
        f"SELECT count() AS n FROM {s3_function(settings, 'Parquet', _RESTORE_SCHEMA)} "
        "WHERE event_time >= fromUnixTimestamp({from_s:Int64})",
        params={"url": days_glob(settings.url, hours), "from_s": hours[0]},
        settings={**S3_SETTINGS, "max_execution_time": "600"},
    )
    return int(result.rows[0]["n"])


async def _restore_raw(ch: ClickHouse, settings: ArchiveSettings, hours: list[int]) -> int:
    """Put these hours' files back into the raw table; the view fills their rollup minutes.

    Restored rows carry no SSE bookmark (empty `sse_id`), which is how ingest recognises
    them. Their `ingest_seq` comes from when they were first ingested, so the most recent
    ones are the seam ingest dedupes the replay against.
    """
    await require_ingest_stopped(ch, quiet=timedelta(0))
    if int((await ch.query(_RAW_ROWS)).rows[0]["n"]):
        raise RuntimeError("raw rows appeared during the restore; is ingest running?")
    expected = await _file_rows(ch, settings, hours)
    await ch.execute(
        f"""
        INSERT INTO wiki_edits
            (event_id, event_time, ingested_at, wiki, lang, type, namespace, title, is_bot,
             sse_id, ingest_seq)
        SELECT toUUID(event_id), event_time, ingested_at, wiki, lang, type, namespace, title,
               is_bot, '', toUInt64(toUnixTimestamp64Nano(ingested_at))
        FROM {s3_function(settings, "Parquet", _RESTORE_SCHEMA)}
        WHERE event_time >= fromUnixTimestamp({{from_s:Int64}})
        """,
        params={"url": days_glob(settings.url, hours), "from_s": hours[0]},
        settings={**REBUILD_INSERT_SETTINGS, **S3_SETTINGS},
    )
    # Raw was empty and ingest isn't running, so everything there now is ours.
    rows = int((await ch.query(_RAW_ROWS)).rows[0]["n"])
    if rows != expected:
        raise RuntimeError(f"restored {rows} raw rows, the files hold {expected}")
    return rows
