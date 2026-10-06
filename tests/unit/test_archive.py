"""The archive's planning and naming, against a stub database."""

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import pytest

from livedemos.archive.job import (
    HOUR_S,
    Archiver,
    days_glob,
    hour_of_path,
    hour_url,
    s3_function,
)
from livedemos.clickhouse import QueryResult, QueryStats
from livedemos.config import ArchiveSettings

BASE = "https://bucket.s3.eu-west-1.amazonaws.com/wikipedia/edits"
H = int(datetime(2026, 10, 6, 9, tzinfo=UTC).timestamp())


def test_hour_url_is_hive_style() -> None:
    assert hour_url(BASE, H) == f"{BASE}/dt=2026-10-06/hour=09.parquet"


def test_days_glob_covers_each_day_once() -> None:
    hours = [H - 10 * HOUR_S, H, H + HOUR_S]  # 2026-10-05 23:00 to 2026-10-06 10:00
    assert days_glob(BASE, hours) == f"{BASE}/dt={{2026-10-05,2026-10-06}}/hour=*.parquet"


@pytest.mark.parametrize(
    ("path", "hour"),
    [
        ("archive/wikipedia/edits/dt=2026-10-06/hour=09.parquet", H),
        ("bucket/wikipedia/edits/dt=2026-10-06/hour=09.parquet.tmp", None),
        ("bucket/elsewhere/report.parquet", None),
    ],
)
def test_hour_of_path(path: str, hour: int | None) -> None:
    assert hour_of_path(path) == hour


def test_s3_function_binds_the_url_and_escapes_the_schema() -> None:
    signed = ArchiveSettings(url=BASE)
    assert s3_function(signed, "Parquet") == "s3({url:String}, 'Parquet')"
    local = ArchiveSettings(url=BASE, nosign=True)
    assert s3_function(local, "One") == "s3({url:String}, NOSIGN, 'One')"
    schema = "event_time DateTime64(3, 'UTC')"
    assert s3_function(signed, "Parquet", schema).endswith(
        "'Parquet', 'event_time DateTime64(3, \\'UTC\\')')"
    )


def test_settings_reject_a_trailing_slash() -> None:
    with pytest.raises(ValueError, match="trailing slash"):
        ArchiveSettings(url=BASE + "/")


class StubWarehouse:
    """Raw rows span `oldest` to `newest`; `files` are the hours already in S3."""

    def __init__(self, *, oldest: int, newest: int, files: set[int]) -> None:
        self.span = {"oldest_s": oldest, "newest_s": newest, "n": 1}
        self.files = files

    async def query(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> QueryResult:
        stats = QueryStats(0, 0, 0)
        if "_path" in sql:
            paths = [{"path": hour_url("bucket", h)} for h in sorted(self.files)]
            return QueryResult(paths, stats)
        return QueryResult([self.span], stats)

    async def execute(self, sql: str, *, params: Any = None, settings: Any = None) -> None:
        raise AssertionError("planning must not write")


async def test_plan_skips_archived_hours_and_ones_ingest_hasnt_passed() -> None:
    now = datetime.fromtimestamp(H + 3 * HOUR_S, UTC)
    stub = StubWarehouse(
        oldest=H - 2 * HOUR_S + 600,  # first rows 10 minutes into H-2
        newest=H + 2 * HOUR_S + 200,  # 200 s into H+2: H+1 ended, but under settle_s ago
        files={H - HOUR_S},
    )
    plan = await Archiver(ArchiveSettings(url=BASE, settle_s=300), stub).plan(now)
    assert plan.due == [H - 2 * HOUR_S, H]
    assert plan.newest_archived == H - HOUR_S


async def test_plan_stays_inside_the_lookback() -> None:
    now = datetime.fromtimestamp(H, UTC)
    stub = StubWarehouse(oldest=H - 30 * HOUR_S, newest=H, files=set())
    settings = ArchiveSettings(url=BASE, settle_s=0, lookback_s=3 * HOUR_S)
    plan = await Archiver(settings, stub).plan(now)
    assert plan.due == [H - 3 * HOUR_S, H - 2 * HOUR_S, H - HOUR_S]
