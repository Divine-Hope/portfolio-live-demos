"""Write each finished hour of raw edits to one Parquet file on S3, and keep it complete.

    <url>/dt=YYYY-MM-DD/hour=HH.parquet

ClickHouse does the work: one `INSERT INTO FUNCTION s3(...) SELECT ...` per hour, signed
with the host's instance role, so no credentials pass through Python.

Which hours: every hour inside the lookback that has finished, that ingest has passed by
`settle_s`, and whose raw rows outnumber what its file holds. `archive_hours` records what
each file holds, counted from the file itself after writing it. So:

- a new hour is written once ingest has passed it;
- an hour that gains rows later (a late event, a replay after an outage) is written
  again, for as long as its raw rows are kept;
- a file that didn't match its raw rows when written is fixed on the next run;
- a file is never replaced by one with fewer rows. A host rebuilt from scratch has fewer
  raw rows than its predecessor wrote; it finds those files (they aren't in its new
  `archive_hours`), records what they hold, and leaves them alone.

"Passed" uses the newest committed event, not the clock: after an outage, ingest replays
the stream oldest first, and an hour waits for the replay to pass it.

Only whole hours: one that started before the first raw row (first boot, a rebuilt host)
or before the retention cutoff (raw rows expire part by part, so its start may be gone)
is never archived as if it were complete.

An hour with no raw rows gets no file. Rewriting an hour by hand, whatever it holds, is
`python -m livedemos.archive --hour ...` (stop the service first).
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from livedemos.archive import metrics
from livedemos.clickhouse import Queryable
from livedemos.config import ArchiveSettings

log = logging.getLogger(__name__)

HOUR_S = 3_600
_FILE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/hour=([01]\d|2[0-3])\.parquet$")

# The archive's columns: the edit itself. The resume bookmark and ingest order (sse_id,
# ingest_seq) are ingest's bookkeeping, not data. The id is a string, not Parquet's UUID
# logical type, so every engine reads it the same way.
_COLUMNS = """
    toString(event_id) AS event_id, event_time, ingested_at, wiki, lang, type,
    namespace, title, is_bot
"""
# What the rollup rebuild reads back. Naming the schema skips inferring it from every file.
READ_SCHEMA = "event_time DateTime64(3, 'UTC'), lang String, is_bot Bool"
_TIME_SCHEMA = "event_time DateTime64(3, 'UTC')"
_PAGES_SCHEMA = "event_time DateTime64(3, 'UTC'), lang String, namespace Int32, title String"
_HOUR_RANGE = (
    "event_time >= fromUnixTimestamp({from_s:Int64}) "
    "AND event_time < fromUnixTimestamp({to_s:Int64})"
)
# The path in an `s3()` URL is matched by ClickHouse's own globbing; hive partitioning
# is off, or ClickHouse would expect `dt` as a column on write.
S3_SETTINGS = {"use_hive_partitioning": "0"}

Result = Literal["written", "mismatch", "error"]


class NothingToArchive(ValueError):
    """A manual rewrite of an hour ClickHouse has no rows for: it would change nothing."""


class FewerRows(RuntimeError):
    """ClickHouse has no more rows for the hour than its file: writing would lose some."""


def _ceil_hour(ms: int) -> int:
    """The start of the first whole hour at or after `ms` (milliseconds), in seconds."""
    return -(-ms // (HOUR_S * 1000)) * HOUR_S


class Warehouse(Queryable, Protocol):
    async def execute(self, sql: str, *, params: Any = None, settings: Any = None) -> None: ...


def hour_url(base: str, hour_s: int) -> str:
    start = datetime.fromtimestamp(hour_s, UTC)
    return f"{base}/dt={start:%Y-%m-%d}/hour={start:%H}.parquet"


def days_glob(base: str, hours: list[int]) -> str:
    """One URL matching every file on the days these hours fall in."""
    days = sorted({f"{datetime.fromtimestamp(h, UTC):%Y-%m-%d}" for h in hours})
    return f"{base}/dt={{{','.join(days)}}}/hour=*.parquet"


def hour_of_path(path: str) -> int | None:
    """The hour a file holds, or None for anything that isn't an hour file."""
    match = _FILE.search(path)
    if not match:
        return None
    try:
        day = datetime.strptime(match[1], "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:  # 2026-02-30
        return None
    return int(day.timestamp()) + int(match[2]) * HOUR_S


def s3_function(settings: ArchiveSettings, fmt: str, structure: str | None = None) -> str:
    """`s3({url:String}, [NOSIGN,] 'Format'[, 'structure'])`: the URL is a bound parameter."""
    args = ["{url:String}"]
    if settings.nosign:
        args.append("NOSIGN")
    args.append(f"'{fmt}'")
    if structure:
        escaped = structure.replace("\\", "\\\\").replace("'", "\\'")
        args.append(f"'{escaped}'")
    return f"s3({', '.join(args)})"


@dataclass(frozen=True, slots=True)
class HourResult:
    hour_s: int
    result: Result
    rows: int  # rows in the file after this run


@dataclass(frozen=True, slots=True)
class Plan:
    due: list[int]  # hours to write, oldest first
    archived: dict[int, int]  # hour -> rows its file holds, for the hours considered
    unreadable: list[int] = field(default_factory=list)  # files found but not readable


class Archiver:
    def __init__(self, settings: ArchiveSettings, ch: Warehouse):
        self._settings = settings
        self._ch = ch

    async def plan(self, now: datetime) -> Plan:
        """Hours that have finished, that ingest has passed, and that their file lacks."""
        span = (await self._ch.query(_RAW_SPAN)).rows[0]
        if not int(span["n"]):
            return Plan(due=[], archived={})
        first = max(
            _ceil_hour(int(span["oldest_ms"])),
            _ceil_hour(int(now.timestamp() * 1000) - self._settings.lookback_s * 1000),
        )
        # Hours that ended at least settle_s before the newest committed event.
        end = (int(span["newest_s"]) - self._settings.settle_s) // HOUR_S * HOUR_S
        if end <= first:
            return Plan(due=[], archived={})
        bounds = {"from_s": first, "to_s": end}
        raw = await self._hour_counts(_RAW_COUNTS, bounds)
        archived = await self._hour_counts(_RECORDED_COUNTS, bounds)
        unreadable: list[int] = []
        unrecorded = [h for h in raw if h not in archived]
        if unrecorded:
            adopted, unreadable = await self._adopt(unrecorded)
            archived |= adopted
        # An unreadable file is never overwritten automatically: it may hold more than
        # this host has. It's reported, and `--hour` replaces it on purpose.
        due = sorted(h for h, n in raw.items() if n > archived.get(h, 0) and h not in unreadable)
        return Plan(due=due, archived=archived, unreadable=unreadable)

    async def existing_hours(self, hours: list[int]) -> set[int]:
        # Format `One` lists the matching files without reading them.
        result = await self._ch.query(
            f"SELECT _path AS path FROM {s3_function(self._settings, 'One')}",
            params={"url": days_glob(self._settings.url, hours)},
            settings=S3_SETTINGS,
        )
        found = {hour_of_path(str(r["path"])) for r in result.rows}
        return {h for h in found if h is not None} & set(hours)

    async def archive_hour(
        self, hour_s: int, *, floor: int = 0, manual: bool = False
    ) -> HourResult:
        """Write one hour's file, replacing any that's there, read it back and record it.

        `floor` is what the file holds now. A scheduled write only goes ahead with more
        rows than that, and never records fewer; `manual` writes whatever ClickHouse has.
        """
        bounds = {"from_s": hour_s, "to_s": hour_s + HOUR_S}
        raw_before = await self._raw_count(bounds)
        if manual and not raw_before:
            raise NothingToArchive("ClickHouse has no rows for that hour; the file is unchanged")
        if not manual and raw_before <= floor:
            raise FewerRows(f"{raw_before} raw rows, the file has {floor}; not rewritten")
        if not manual and await self.existing_hours([hour_s]):
            # Read the file itself too, not only this host's record of it: while the Auto
            # Scaling Group replaces the host, two hosts archive at once, and the other
            # may have written more since (ADR 0010).
            in_file = await self._checked_count(hour_s)
            if raw_before <= in_file:
                await self._record(hour_s, in_file)
                raise FewerRows(f"{raw_before} raw rows, the file has {in_file}; not rewritten")
        url = hour_url(self._settings.url, hour_s)
        await self._ch.execute(
            f"INSERT INTO FUNCTION {s3_function(self._settings, 'Parquet')} "
            f"SELECT {_COLUMNS} FROM wiki_edits WHERE {_HOUR_RANGE} "
            "ORDER BY event_time, event_id",
            params={"url": url, **bounds},
            settings={
                **S3_SETTINGS,
                "s3_truncate_on_insert": "1",  # replaces the file
                "output_format_parquet_compression_method": "zstd",
            },
        )
        in_file = await self._file_count(url)
        # Always the truth, so a later run compares against what the file really holds.
        await self._record(hour_s, in_file)
        await self._complete_pages(bounds)
        if in_file < floor and not manual:
            # Rows vanished between the count and the write (deleted by hand: the hours
            # considered are inside retention). The previous version is in the bucket.
            log.error("an hour's file now has fewer rows", extra={"hour_s": hour_s})
            return HourResult(hour_s, "mismatch", in_file)
        # Counted after the read-back: a row that landed in between is a mismatch now, and
        # the next run writes the hour again because its raw rows outnumber the file's.
        raw = await self._raw_count(bounds)
        return HourResult(hour_s, "written" if in_file == raw else "mismatch", in_file)

    async def run_once(self, now: datetime) -> list[HourResult]:
        """Archive every due hour. One hour failing doesn't stop the others."""
        plan = await self.plan(now)
        archived = dict(plan.archived)
        results: list[HourResult] = []
        for hour_s in plan.due:
            hour = datetime.fromtimestamp(hour_s, UTC).isoformat()
            try:
                outcome = await self.archive_hour(hour_s, floor=archived.get(hour_s, 0))
            except Exception:  # ClickHouse or S3; the hour stays due
                outcome = HourResult(hour_s, "error", archived.get(hour_s, 0))
                log.exception("archiving an hour failed", extra={"hour": hour})
            else:
                archived[hour_s] = outcome.rows
                if outcome.result == "written":
                    metrics.ROWS.inc(outcome.rows)
                    log.info("archived hour", extra={"hour": hour, "rows": outcome.rows})
                else:
                    log.error("archived hour doesn't match raw rows", extra={"hour": hour})
            results.append(outcome)
            metrics.HOURS.labels(result=outcome.result).inc()
        unfilled = await self._complete_unfilled(archived, plan.due)
        behind = sorted(
            [r.hour_s for r in results if r.result != "written"] + plan.unreadable + unfilled
        )
        metrics.BEHIND.set(len(behind))
        metrics.OLDEST_BEHIND.set(behind[0] if behind else 0)
        if archived:
            metrics.NEWEST_HOUR.set(max(archived))
        if not behind:
            metrics.LAST_SUCCESS.set(time.time())
        return results

    async def _adopt(self, hours: list[int]) -> tuple[dict[int, int], list[int]]:
        """Files with no record (a rebuilt host): record what they hold, from the files.

        Returns (hour -> rows, hours whose file couldn't be read)."""
        adopted: dict[int, int] = {}
        unreadable: list[int] = []
        for hour_s in sorted(await self.existing_hours(hours)):
            hour = datetime.fromtimestamp(hour_s, UTC).isoformat()
            try:
                rows = await self._checked_count(hour_s)
            except Exception:
                unreadable.append(hour_s)
                log.exception("an archive file can't be read", extra={"hour": hour})
                continue
            await self._record(hour_s, rows)
            adopted[hour_s] = rows
            log.info("found an unrecorded file", extra={"hour": hour, "rows": rows})
        return adopted, unreadable

    async def _complete_unfilled(self, archived: dict[int, int], done: list[int]) -> list[int]:
        """Complete the page sets of archived hours not recorded as complete: one whose
        completion failed after its file was written, or a file this host adopted. Returns
        the hours it couldn't complete; they're tried again next run."""
        candidates = sorted(h for h in archived if h not in done)
        if not candidates:
            return []
        result = await self._ch.query(
            "SELECT DISTINCT toUnixTimestamp(hour) AS h FROM wiki_pages_filled "
            "WHERE toUnixTimestamp(hour) IN {hours:Array(UInt32)}",
            params={"hours": candidates},
        )
        filled = {int(r["h"]) for r in result.rows}
        failed = []
        for hour_s in (h for h in candidates if h not in filled):
            bounds = {"from_s": hour_s, "to_s": hour_s + HOUR_S}
            try:
                # A file this host adopted can hold rows its raw table never had: then the
                # file is the fuller source.
                if await self._raw_count(bounds) < archived[hour_s]:
                    await self._complete_pages_from_file(hour_s)
                else:
                    await self._complete_pages(bounds)
            except Exception:
                failed.append(hour_s)
                log.exception("completing an hour's page sets failed", extra={"hour_s": hour_s})
        return failed

    async def _complete_pages(self, bounds: dict[str, int]) -> None:
        """Complete the hour's page sets (migration 0004) from the rows just archived, and
        record the hour as filled. Their view fills them on every insert, but a view's
        block can be lost on its own (a crash between tables); this checks every hour
        once it's final. Sets only add, so it can't overcount."""
        await self._ch.execute(
            "INSERT INTO wiki_pages_per_minute (minute, lang, pages) "
            "SELECT toStartOfMinute(event_time) AS minute, lang, uniqExactState(namespace, title) "
            f"FROM wiki_edits WHERE {_HOUR_RANGE} GROUP BY minute, lang",
            params=bounds,
        )
        await self._ch.execute(
            "INSERT INTO wiki_pages_filled (hour, filled_at) "
            "SELECT fromUnixTimestamp({from_s:Int64}), now64(6)",
            params=bounds,
        )

    async def _complete_pages_from_file(self, hour_s: int) -> None:
        await self._ch.execute(
            "INSERT INTO wiki_pages_per_minute (minute, lang, pages) "
            "SELECT toStartOfMinute(event_time) AS minute, lang, uniqExactState(namespace, title) "
            f"FROM {s3_function(self._settings, 'Parquet', _PAGES_SCHEMA)} GROUP BY minute, lang",
            params={"url": hour_url(self._settings.url, hour_s)},
            settings=S3_SETTINGS,
        )
        await self._ch.execute(
            "INSERT INTO wiki_pages_filled (hour, filled_at) "
            "SELECT fromUnixTimestamp({hour_s:Int64}), now64(6)",
            params={"hour_s": hour_s},
        )

    async def _record(self, hour_s: int, rows: int) -> None:
        await self._ch.execute(
            "INSERT INTO archive_hours (hour, rows, written_at) "
            "SELECT fromUnixTimestamp({hour_s:Int64}), {rows:UInt64}, now64(6)",
            params={"hour_s": hour_s, "rows": rows},
        )

    async def _hour_counts(self, sql: str, bounds: dict[str, int]) -> dict[int, int]:
        result = await self._ch.query(sql, params=bounds)
        return {int(r["h"]): int(r["n"]) for r in result.rows}

    async def _raw_count(self, bounds: dict[str, int]) -> int:
        result = await self._ch.query(
            f"SELECT count() AS n FROM wiki_edits WHERE {_HOUR_RANGE}", params=bounds
        )
        return int(result.rows[0]["n"])

    async def _checked_count(self, hour_s: int) -> int:
        """Rows in a file this host didn't write, after checking they're all its hour's."""
        result = await self._ch.query(
            "SELECT count() AS n, toUnixTimestamp(min(event_time)) AS lo, "
            "toUnixTimestamp(max(event_time)) AS hi "
            f"FROM {s3_function(self._settings, 'Parquet', _TIME_SCHEMA)}",
            params={"url": hour_url(self._settings.url, hour_s)},
            settings=S3_SETTINGS,
        )
        row = result.rows[0]
        n = int(row["n"])
        if n and not hour_s <= int(row["lo"]) <= int(row["hi"]) < hour_s + HOUR_S:
            raise ValueError("the file holds events from outside its hour")
        return n

    async def _file_count(self, url: str) -> int:
        result = await self._ch.query(
            f"SELECT count() AS n FROM {s3_function(self._settings, 'Parquet')}",
            params={"url": url},
            settings=S3_SETTINGS,
        )
        return int(result.rows[0]["n"])


_RAW_SPAN = """
SELECT toUnixTimestamp64Milli(min(event_time)) AS oldest_ms,
       toUnixTimestamp(max(event_time)) AS newest_s,
       count() AS n
FROM wiki_edits
"""
_RAW_COUNTS = f"""
SELECT toUnixTimestamp(toStartOfHour(event_time)) AS h, count() AS n
FROM wiki_edits WHERE {_HOUR_RANGE}
GROUP BY h
"""
_RECORDED_COUNTS = """
SELECT toUnixTimestamp(hour) AS h, argMax(rows, (written_at, rows)) AS n
FROM archive_hours
WHERE hour >= fromUnixTimestamp({from_s:Int64}) AND hour < fromUnixTimestamp({to_s:Int64})
GROUP BY h
"""
