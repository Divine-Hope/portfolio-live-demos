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
from livedemos.clickhouse import ClickHouseError, QueryResult, QueryStats
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
        ("bucket/wikipedia/edits/dt=2026-10-06/hour=24.parquet", None),
        ("bucket/wikipedia/edits/dt=2026-02-30/hour=09.parquet", None),
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
    """Raw rows per hour, `archive_hours` records, and files in S3 (hour -> rows)."""

    def __init__(
        self,
        *,
        raw: dict[int, int],
        recorded: dict[int, int] | None = None,
        files: dict[int, int] | None = None,
        unreadable: set[int] | None = None,
        oldest: int | None = None,  # the first raw row; the start of the first hour if None
    ) -> None:
        self.raw = raw
        self.oldest = oldest if oldest is not None else min(raw)
        self.recorded = dict(recorded or {})
        self.files = dict(files or {})
        self.unreadable = unreadable or set()
        self.writes: list[str] = []

    async def query(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> QueryResult:
        stats = QueryStats(0, 0, 0)
        params = params or {}
        if "url" in params and "count()" in sql:  # reading a file
            hour_s = hour_of_path(str(params["url"]))
            assert hour_s is not None
            if hour_s in self.unreadable:
                raise ClickHouseError("Cannot read Parquet")
            row = {"n": self.files[hour_s], "lo": hour_s, "hi": hour_s + 60}
            return QueryResult([row], stats)
        if "min(event_time)" in sql:
            span = {"oldest_s": self.oldest, "newest_s": max(self.raw) + HOUR_S, "n": 1}
            return QueryResult([span], stats)
        if "FROM archive_hours" in sql:
            return QueryResult([{"h": h, "n": n} for h, n in self.recorded.items()], stats)
        if "toStartOfHour(event_time)" in sql:
            lo, hi = params["from_s"], params["to_s"]
            rows = [{"h": h, "n": n} for h, n in self.raw.items() if lo <= h < hi]
            return QueryResult(rows, stats)
        if "_path" in sql:
            return QueryResult([{"path": hour_url("bucket", h)} for h in self.files], stats)
        raise AssertionError(f"unexpected query: {sql}")

    async def execute(self, sql: str, *, params: Any = None, settings: Any = None) -> None:
        assert "INSERT INTO archive_hours" in sql, "planning only records what it finds"
        self.recorded[params["hour_s"]] = params["rows"]
        self.writes.append(sql)


def settings(**overrides: Any) -> ArchiveSettings:
    return ArchiveSettings(**{"url": BASE, "settle_s": 0, **overrides})


NOW = datetime.fromtimestamp(H + 6 * HOUR_S, UTC)


async def test_plan_writes_hours_whose_file_holds_fewer_rows_than_clickhouse() -> None:
    stub = StubWarehouse(
        raw={H - 2 * HOUR_S: 5, H - HOUR_S: 3, H: 4},
        recorded={H - HOUR_S: 3, H: 2},  # H gained two late rows since it was written
    )
    plan = await Archiver(settings(), stub).plan(NOW)
    assert plan.due == [H - 2 * HOUR_S, H]


async def test_plan_never_replaces_a_file_with_fewer_rows() -> None:
    """A host rebuilt from scratch: its predecessor's file isn't in archive_hours."""
    stub = StubWarehouse(raw={H: 4}, files={H: 9})
    plan = await Archiver(settings(), stub).plan(NOW)
    assert plan.due == []
    assert plan.archived == {H: 9}
    assert stub.recorded == {H: 9}  # adopted, so it isn't read again


async def test_an_unreadable_file_is_reported_not_overwritten() -> None:
    stub = StubWarehouse(raw={H - HOUR_S: 4, H: 4}, files={H - HOUR_S: 1, H: 9}, unreadable={H})
    plan = await Archiver(settings(), stub).plan(NOW)
    assert plan.due == [H - HOUR_S]
    assert plan.unreadable == [H]


async def test_plan_waits_for_ingest_to_pass_an_hour() -> None:
    stub = StubWarehouse(raw={H - HOUR_S: 4, H: 4})  # newest event at the end of H
    plan = await Archiver(settings(settle_s=300), stub).plan(NOW)
    assert plan.due == [H - HOUR_S]


async def test_plan_stays_inside_the_lookback() -> None:
    raw = {H - n * HOUR_S: 1 for n in range(30)}
    plan = await Archiver(settings(lookback_s=3 * HOUR_S), StubWarehouse(raw=raw)).plan(
        datetime.fromtimestamp(H, UTC)
    )
    assert plan.due == [H - 3 * HOUR_S, H - 2 * HOUR_S, H - HOUR_S, H]


async def test_plan_skips_an_hour_that_started_before_the_first_raw_row() -> None:
    """First boot, or a rebuilt host: rows begin 10 minutes into H."""
    stub = StubWarehouse(raw={H: 4, H + HOUR_S: 4}, oldest=H + 600)
    plan = await Archiver(settings(), stub).plan(datetime.fromtimestamp(H + 3 * HOUR_S, UTC))
    assert plan.due == [H + HOUR_S]


async def test_plan_skips_the_hour_the_retention_cutoff_falls_in() -> None:
    raw = {H - n * HOUR_S: 1 for n in range(10)}
    now = datetime.fromtimestamp(H + 1800, UTC)  # cutoff lands 30 minutes into H - 3h
    plan = await Archiver(settings(lookback_s=3 * HOUR_S), StubWarehouse(raw=raw)).plan(now)
    assert plan.due == [H - 2 * HOUR_S, H - HOUR_S, H]
