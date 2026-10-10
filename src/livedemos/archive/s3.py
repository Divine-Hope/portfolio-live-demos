"""Where the archive's hour files live, and how ClickHouse's s3() reaches them.

<url>/dt=YYYY-MM-DD/hour=HH.parquet
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from livedemos.config import ArchiveSettings
from livedemos.db.clickhouse import Queryable, quote

HOUR_S = 3_600
_FILE = re.compile(r"dt=(\d{4}-\d{2}-\d{2})/hour=([01]\d|2[0-3])\.parquet$")

# The path in an `s3()` URL is matched by ClickHouse's own globbing; hive partitioning
# is off, or ClickHouse would expect `dt` as a column on write.
S3_SETTINGS = {"use_hive_partitioning": "0"}


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
        args.append(quote(structure))
    return f"s3({', '.join(args)})"


async def existing_hours(ch: Queryable, settings: ArchiveSettings, hours: list[int]) -> set[int]:
    """Which of these hours have a file. Format `One` lists the files without reading them."""
    result = await ch.query(
        f"SELECT _path AS path FROM {s3_function(settings, 'One')}",
        params={"url": days_glob(settings.url, hours)},
        settings=S3_SETTINGS,
    )
    found = {hour_of_path(str(r["path"])) for r in result.rows}
    return {h for h in found if h is not None} & set(hours)
