"""The daily Cost Explorer fetch (ops/cost.py), against stubs: no AWS, no ClickHouse.

The rule under test: Cost Explorer is asked at most once per UTC day, whatever happens to
the process, the host, or a second host running at the same time.
"""

import io
import json
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
NOW = datetime(2026, 10, 8, 22, 30, tzinfo=UTC)
CLOCK = {"now": NOW}  # what the fetcher's clock says: the time each test asks at


def at(when: datetime) -> datetime:
    CLOCK["now"] = when
    return when


class StubDatabase:
    """One host's ClickHouse: `aws_cost` rows, and whether it's answering."""

    def __init__(self) -> None:
        self.rows: list[Mapping[str, Any]] = []
        self.fail_query = False
        self.fail_inserts = 0

    async def query(self, sql: str, *, params: Mapping[str, Any], **_: Any) -> QueryResult:
        if self.fail_query:
            raise ClickHouseError("down")
        day = params["day"]
        n = sum(1 for r in self.rows if str(r["fetched_at"]).startswith(day))
        return QueryResult([{"n": n}], QueryStats(0.0, 0, 0))

    async def insert(self, table: str, rows: Sequence[Mapping[str, Any]], **_: Any) -> None:
        assert table == "aws_cost"
        if self.fail_inserts:
            self.fail_inserts -= 1
            raise ClickHouseError("insert failed")
        self.rows.extend(rows)


class PreconditionFailed(Exception):
    def __init__(self) -> None:
        super().__init__("PreconditionFailed")
        self.response = {"Error": {"Code": "PreconditionFailed"}}


class StubS3:
    """The archive bucket, shared by every host: conditional puts like S3's."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs: Any) -> None:
        if kwargs.get("IfNoneMatch") == "*" and Key in self.objects:
            raise PreconditionFailed()
        self.objects[Key] = Body

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": io.BytesIO(self.objects[Key])}


class StubCostExplorer:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    def get_cost_and_usage(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("AccessDenied")
        return RESPONSE


def fetcher(db: StubDatabase, s3: StubS3, ce: StubCostExplorer) -> cost.CostFetcher:
    return cost.CostFetcher(
        db,  # type: ignore[arg-type]
        tag="project=livedemos",
        claims="s3://archive-bucket/ops/cost",
        ce_factory=lambda: ce,
        s3_factory=lambda: s3,
        clock=lambda: CLOCK["now"],
    )


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


async def test_asks_once_a_day_and_records_the_answer_in_both_places() -> None:
    db, s3, ce = StubDatabase(), StubS3(), StubCostExplorer()
    f = fetcher(db, s3, ce)
    assert await f.refresh_if_due(at(NOW)) is True
    assert await f.refresh_if_due(at(NOW + timedelta(hours=1))) is False
    assert len(ce.calls) == 1
    assert (db.rows[0]["amount"], db.rows[0]["currency"]) == ("0.1977203052", "USD")
    claim = json.loads(s3.objects["ops/cost/2026-10-08.json"])
    assert claim["result"]["amount"] == "0.1977203052"
    # A new UTC day asks again.
    assert await f.refresh_if_due(at(NOW + timedelta(hours=2))) is True
    assert len(ce.calls) == 2


async def test_a_second_host_the_same_day_copies_the_answer_instead_of_asking() -> None:
    s3, ce = StubS3(), StubCostExplorer()
    first, replacement = StubDatabase(), StubDatabase()  # each host has its own ClickHouse
    assert await fetcher(first, s3, ce).refresh_if_due(at(NOW)) is True
    assert (
        await fetcher(replacement, s3, ce).refresh_if_due(at(NOW + timedelta(minutes=30))) is False
    )
    assert len(ce.calls) == 1
    assert replacement.rows[0]["amount"] == "0.1977203052"


async def test_a_claim_without_an_answer_means_no_second_request() -> None:
    # The winner died between claiming and answering: no number today, never a second call.
    s3, ce = StubS3(), StubCostExplorer()
    s3.objects["ops/cost/2026-10-08.json"] = json.dumps({"claimed_at": "x"}).encode()
    db = StubDatabase()
    f = fetcher(db, s3, ce)
    assert await f.refresh_if_due(at(NOW)) is False
    assert await f.refresh_if_due(at(NOW + timedelta(minutes=5))) is False
    assert ce.calls == []
    assert db.rows == []


async def test_a_failed_request_is_recorded_and_not_retried_the_same_day() -> None:
    db, s3, ce = StubDatabase(), StubS3(), StubCostExplorer(fail=True)
    f = fetcher(db, s3, ce)
    assert await f.refresh_if_due(at(NOW)) is True
    assert await f.refresh_if_due(at(NOW + timedelta(minutes=5))) is False
    assert await fetcher(StubDatabase(), s3, ce).refresh_if_due(at(NOW)) is False  # after a restart
    assert len(ce.calls) == 1
    assert db.rows[0]["ok"] is False
    assert "AccessDenied" in db.rows[0]["error"]


async def test_a_lost_insert_is_filled_from_the_claim_not_a_second_request() -> None:
    db, s3, ce = StubDatabase(), StubS3(), StubCostExplorer()
    db.fail_inserts = 1
    f = fetcher(db, s3, ce)
    assert await f.refresh_if_due(at(NOW)) is True
    assert db.rows == []
    assert await f.refresh_if_due(at(NOW + timedelta(minutes=5))) is False
    assert len(ce.calls) == 1
    assert db.rows[0]["amount"] == "0.1977203052"


async def test_an_answer_that_couldnt_be_saved_anywhere_is_kept_and_saved_later() -> None:
    class FlakyS3(StubS3):
        fail_result = True

        def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs: Any) -> None:
            if "IfNoneMatch" not in kwargs and self.fail_result:
                self.fail_result = False
                raise RuntimeError("SlowDown")
            super().put_object(Bucket=Bucket, Key=Key, Body=Body, **kwargs)

    db, s3, ce = StubDatabase(), FlakyS3(), StubCostExplorer()
    db.fail_inserts = 1
    f = fetcher(db, s3, ce)
    assert await f.refresh_if_due(at(NOW)) is True
    assert db.rows == []
    assert "result" not in json.loads(s3.objects["ops/cost/2026-10-08.json"])
    assert await f.refresh_if_due(at(NOW + timedelta(minutes=5))) is False
    assert len(ce.calls) == 1
    assert db.rows[0]["amount"] == "0.1977203052"
    # A replacement host later today finds the answer in the claim.
    assert json.loads(s3.objects["ops/cost/2026-10-08.json"])["result"]["currency"] == "USD"


async def test_no_request_when_midnight_passes_while_claiming() -> None:
    class SlowS3(StubS3):
        def put_object(self, **kwargs: Any) -> None:
            super().put_object(**kwargs)
            CLOCK["now"] = datetime(2026, 10, 9, 0, 0, 1, tzinfo=UTC)  # the claim took a while

    db, ce = StubDatabase(), StubCostExplorer()
    f = cost.CostFetcher(
        db,  # type: ignore[arg-type]
        tag="project=livedemos",
        claims="s3://archive-bucket/ops/cost",
        ce_factory=lambda: ce,
        s3_factory=SlowS3,
        clock=lambda: CLOCK["now"],
    )
    assert await f.refresh_if_due(at(datetime(2026, 10, 8, 23, 59, 59, tzinfo=UTC))) is False
    assert ce.calls == []


async def test_no_request_while_clickhouse_cant_say_whether_today_was_done() -> None:
    db, s3, ce = StubDatabase(), StubS3(), StubCostExplorer()
    db.fail_query = True
    assert await fetcher(db, s3, ce).refresh_if_due(at(NOW)) is False
    assert ce.calls == []
    assert s3.objects == {}


async def test_no_request_when_the_claim_cant_be_written() -> None:
    class BrokenS3(StubS3):
        def put_object(self, **kwargs: Any) -> None:
            raise RuntimeError("AccessDenied")

    ce = StubCostExplorer()
    f = cost.CostFetcher(
        StubDatabase(),  # type: ignore[arg-type]
        tag="project=livedemos",
        claims="s3://archive-bucket/ops/cost",
        ce_factory=lambda: ce,
        s3_factory=BrokenS3,
        clock=lambda: CLOCK["now"],
    )
    assert await f.refresh_if_due(at(NOW)) is False
    assert ce.calls == []


@pytest.mark.parametrize(
    ("tag", "claims"),
    [("livedemos", "s3://b/ops/cost"), ("project=livedemos", "b/ops"), ("project=x", "s3://b")],
)
def test_settings_are_checked(tag: str, claims: str) -> None:
    with pytest.raises(ValueError, match=r"tag|claims"):
        cost.CostFetcher(
            StubDatabase(),  # type: ignore[arg-type]
            tag=tag,
            claims=claims,
            ce_factory=object,
            s3_factory=object,
        )
