"""The daily Cost Explorer fetch (ops/cost.py), against stubs: no AWS, no ClickHouse."""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from livedemos.clickhouse import ClickHouseError, QueryResult, QueryStats
from livedemos.ops import cost

RESPONSE = {
    "ResultsByTime": [
        {
            "TimePeriod": {"Start": "2026-10-01", "End": "2026-10-09"},
            "Total": {"UnblendedCost": {"Amount": "0.1977203052", "Unit": "USD"}},
            "Groups": [],
            "Estimated": True,
        }
    ],
}


class StubDatabase:
    def __init__(self, *, attempted_today: int = 0, fail_query: bool = False) -> None:
        self.attempted_today = attempted_today
        self.fail_query = fail_query
        self.fail_inserts = 0
        self.rows: list[Mapping[str, Any]] = []

    async def query(self, sql: str, **_: Any) -> QueryResult:
        if self.fail_query:
            raise ClickHouseError("down")
        return QueryResult([{"n": self.attempted_today}], QueryStats(0.0, 0, 0))

    async def insert(self, table: str, rows: Sequence[Mapping[str, Any]], **_: Any) -> None:
        if self.fail_inserts:
            self.fail_inserts -= 1
            raise ClickHouseError("insert failed")
        assert table == "aws_cost"
        self.rows.extend(rows)


class StubCostExplorer:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    def get_cost_and_usage(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("AccessDenied")
        return RESPONSE


NOW = datetime(2026, 10, 8, 22, 30, tzinfo=UTC)


def fetcher(db: StubDatabase, ce: StubCostExplorer) -> cost.CostFetcher:
    return cost.CostFetcher(db, tag="project=livedemos", client_factory=lambda: ce)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("today", "expected"),
    [
        (date(2026, 10, 8), (date(2026, 10, 1), date(2026, 10, 9))),
        (date(2026, 10, 1), (date(2026, 10, 1), date(2026, 10, 2))),
        (date(2026, 12, 31), (date(2026, 12, 1), date(2027, 1, 1))),
    ],
)
def test_the_period_is_the_month_so_far_with_an_exclusive_end(
    today: date, expected: tuple[date, date]
) -> None:
    assert cost.period(today) == expected


def test_the_request_filters_by_the_project_tag() -> None:
    req = cost.request(date(2026, 10, 8), "project", "livedemos")
    assert req["Filter"] == {"Tags": {"Key": "project", "Values": ["livedemos"]}}
    assert req["Metrics"] == ["UnblendedCost"]
    assert req["TimePeriod"] == {"Start": "2026-10-01", "End": "2026-10-09"}


def test_parse_keeps_the_amount_and_currency_exactly_as_aws_sent_them() -> None:
    assert cost.parse(RESPONSE) == ("0.1977203052", "USD", True)
    with pytest.raises(ValueError, match="one month"):
        cost.parse({"ResultsByTime": []})


async def test_asks_once_a_day_and_records_the_answer() -> None:
    db, ce = StubDatabase(), StubCostExplorer()
    f = fetcher(db, ce)
    assert await f.refresh_if_due(NOW) is True
    assert await f.refresh_if_due(NOW + timedelta(hours=1)) is False
    assert len(ce.calls) == 1
    assert db.rows[0]["ok"] is True
    assert (db.rows[0]["amount"], db.rows[0]["currency"]) == ("0.1977203052", "USD")
    # A new UTC day asks again.
    assert await f.refresh_if_due(NOW + timedelta(hours=2)) is True
    assert len(ce.calls) == 2


async def test_an_attempt_already_in_the_table_counts_after_a_restart() -> None:
    db, ce = StubDatabase(attempted_today=1), StubCostExplorer()
    assert await fetcher(db, ce).refresh_if_due(NOW) is False
    assert ce.calls == []


async def test_a_failed_request_is_recorded_and_not_retried_the_same_day() -> None:
    db, ce = StubDatabase(), StubCostExplorer(fail=True)
    f = fetcher(db, ce)
    assert await f.refresh_if_due(NOW) is True
    assert await f.refresh_if_due(NOW + timedelta(minutes=5)) is False
    assert len(ce.calls) == 1
    assert db.rows[0]["ok"] is False
    assert "AccessDenied" in db.rows[0]["error"]


async def test_a_lost_insert_doesnt_cause_a_second_request() -> None:
    db, ce = StubDatabase(), StubCostExplorer()
    db.fail_inserts = 1
    f = fetcher(db, ce)
    assert await f.refresh_if_due(NOW) is True
    assert await f.refresh_if_due(NOW + timedelta(minutes=5)) is False
    assert len(ce.calls) == 1


async def test_no_request_while_clickhouse_cant_say_whether_today_was_done() -> None:
    db, ce = StubDatabase(fail_query=True), StubCostExplorer()
    assert await fetcher(db, ce).refresh_if_due(NOW) is False
    assert ce.calls == []


def test_the_tag_must_be_key_equals_value() -> None:
    with pytest.raises(ValueError, match="key=value"):
        cost.CostFetcher(StubDatabase(), tag="livedemos", client_factory=object)  # type: ignore[arg-type]
