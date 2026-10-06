"""Write each finished hour of raw edits to one Parquet file on S3.

    <url>/dt=YYYY-MM-DD/hour=HH.parquet

ClickHouse does the work: one `INSERT INTO FUNCTION s3(...) SELECT ...` per hour, signed
with the host's instance role, so no credentials pass through Python.

Which hours: every hour inside the lookback that has finished, that ingest has moved past
by `settle_s`, and that has no file yet. So the job only ever adds files. A host rebuilt
from scratch has fewer raw rows for recent hours than the host before it; it must not
replace that host's complete files with thinner ones. Overwriting an hour is a deliberate,
manual act (`python -m livedemos.archive --hour ...`).

"Moved past" uses the newest committed event, not the clock. After an outage, ingest
replays the stream from its bookmark, oldest first, and an hour is archived only once the
replay has passed it.

An hour with no raw rows gets no file: there is nothing to keep, and it stays eligible in
case a replay fills it. Every file written is read back and its row count compared with
ClickHouse's.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from livedemos.archive import metrics
from livedemos.clickhouse import Queryable
from livedemos.config import ArchiveSettings

log = logging.getLogger(__name__)

HOUR_S = 3_600
_FILE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/hour=(\d{2})\.parquet$")

# The archive's columns: the edit itself. The resume bookmark and ingest order (sse_id,
# ingest_seq) are ingest's bookkeeping, not data. Parquet has no UUID type.
_COLUMNS = """
    toString(event_id) AS event_id, event_time, ingested_at, wiki, lang, type,
    namespace, title, is_bot
"""
# What the rollup rebuild reads back. Naming the schema skips inferring it from every file.
READ_SCHEMA = "event_time DateTime64(3, 'UTC'), lang String, is_bot Bool"
_HOUR_RANGE = (
    "event_time >= fromUnixTimestamp({from_s:Int64}) "
    "AND event_time < fromUnixTimestamp({to_s:Int64})"
)
# The path in an `s3()` URL is matched by ClickHouse's own globbing; hive partitioning
# is off, or ClickHouse would expect `dt` as a column on write.
_S3_SETTINGS = {"use_hive_partitioning": "0"}

Result = Literal["written", "empty", "mismatch", "error"]


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
    match = _FILE.search(path)
    if not match:
        return None
    day = datetime.strptime(match[1], "%Y-%m-%d").replace(tzinfo=UTC)
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
    rows: int


@dataclass(frozen=True, slots=True)
class Plan:
    due: list[int]  # hours to write, oldest first
    newest_archived: int | None  # newest hour already in S3, among those considered


class Archiver:
    def __init__(self, settings: ArchiveSettings, ch: Warehouse):
        self._settings = settings
        self._ch = ch

    async def plan(self, now: datetime) -> Plan:
        """Hours that have finished, that ingest has passed, and that have no file yet."""
        span = (await self._ch.query(_RAW_SPAN)).rows[0]
        if not int(span["n"]):
            return Plan(due=[], newest_archived=None)
        first = max(
            int(span["oldest_s"]) // HOUR_S * HOUR_S,
            (int(now.timestamp()) - self._settings.lookback_s) // HOUR_S * HOUR_S,
        )
        # Hours that ended at least settle_s before the newest committed event.
        end = (int(span["newest_s"]) - self._settings.settle_s) // HOUR_S * HOUR_S
        hours = list(range(first, end, HOUR_S))
        if not hours:
            return Plan(due=[], newest_archived=None)
        existing = await self.existing_hours(hours)
        return Plan(
            due=[h for h in hours if h not in existing],
            newest_archived=max(existing & set(hours), default=None),
        )

    async def existing_hours(self, hours: list[int]) -> set[int]:
        # Format `One` lists the matching files without reading them.
        result = await self._ch.query(
            f"SELECT _path AS path FROM {s3_function(self._settings, 'One')}",
            params={"url": days_glob(self._settings.url, hours)},
            settings=_S3_SETTINGS,
        )
        found = {hour_of_path(str(r["path"])) for r in result.rows}
        return {h for h in found if h is not None}

    async def archive_hour(self, hour_s: int) -> HourResult:
        """Write one hour's file, replacing any that's there, and check it."""
        bounds = {"from_s": hour_s, "to_s": hour_s + HOUR_S}
        if not await self._raw_count(bounds):
            return HourResult(hour_s, "empty", 0)
        url = hour_url(self._settings.url, hour_s)
        await self._ch.execute(
            f"INSERT INTO FUNCTION {s3_function(self._settings, 'Parquet')} "
            f"SELECT {_COLUMNS} FROM wiki_edits WHERE {_HOUR_RANGE} "
            "ORDER BY event_time, event_id",
            params={"url": url, **bounds},
            settings={
                **_S3_SETTINGS,
                "s3_truncate_on_insert": "1",  # rerunning an hour replaces its file
                "output_format_parquet_compression_method": "zstd",
            },
        )
        in_file = await self._file_count(url)
        # Counted after writing: a late row that landed in between shows up as a mismatch.
        raw = await self._raw_count(bounds)
        return HourResult(hour_s, "written" if in_file == raw else "mismatch", in_file)

    async def run_once(self, now: datetime) -> list[HourResult]:
        """Archive every due hour. One hour failing doesn't stop the others."""
        plan = await self.plan(now)
        newest = plan.newest_archived
        results: list[HourResult] = []
        failed = 0
        for hour_s in plan.due:
            hour = datetime.fromtimestamp(hour_s, UTC).isoformat()
            try:
                outcome = await self.archive_hour(hour_s)
            except Exception:  # ClickHouse or S3; the hour stays due for the next run
                failed += 1
                results.append(HourResult(hour_s, "error", 0))
                metrics.HOURS.labels(result="error").inc()
                log.exception("archiving an hour failed", extra={"hour": hour})
                continue
            results.append(outcome)
            metrics.HOURS.labels(result=outcome.result).inc()
            if outcome.result == "empty":
                continue
            if outcome.result == "written":
                metrics.ROWS.inc(outcome.rows)
                newest = max(newest or hour_s, hour_s)
                log.info("archived hour", extra={"hour": hour, "rows": outcome.rows})
            else:
                failed += 1
                log.error("archived hour doesn't match raw rows", extra={"hour": hour})
        if newest is not None:
            metrics.NEWEST_HOUR.set(newest)
        metrics.FAILED.set(failed)
        return results

    async def _raw_count(self, bounds: dict[str, int]) -> int:
        result = await self._ch.query(
            f"SELECT count() AS n FROM wiki_edits WHERE {_HOUR_RANGE}", params=bounds
        )
        return int(result.rows[0]["n"])

    async def _file_count(self, url: str) -> int:
        result = await self._ch.query(
            f"SELECT count() AS n FROM {s3_function(self._settings, 'Parquet')}",
            params={"url": url},
            settings=_S3_SETTINGS,
        )
        return int(result.rows[0]["n"])


_RAW_SPAN = """
SELECT toUnixTimestamp(min(event_time)) AS oldest_s,
       toUnixTimestamp(max(event_time)) AS newest_s,
       count() AS n
FROM wiki_edits
"""
