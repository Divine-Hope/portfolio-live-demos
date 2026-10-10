"""The rollup's date maths and lock message (rollup/), without a ClickHouse."""

from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from livedemos.db.clickhouse import QueryResult
from livedemos.rollup import lock, reconcile, restore
from tests.stubs import NO_STATS, StubClickHouse


@pytest.mark.parametrize(
    ("first", "end", "pieces"),
    [
        (date(2026, 10, 3), date(2026, 10, 5), [(date(2026, 10, 3), date(2026, 10, 5))]),
        (
            date(2026, 9, 28),
            date(2026, 10, 2),
            [(date(2026, 9, 28), date(2026, 10, 1)), (date(2026, 10, 1), date(2026, 10, 2))],
        ),
        (  # over a year end
            date(2026, 12, 30),
            date(2027, 1, 2),
            [(date(2026, 12, 30), date(2027, 1, 1)), (date(2027, 1, 1), date(2027, 1, 2))],
        ),
        (date(2026, 10, 5), date(2026, 10, 5), []),
    ],
)
def test_months_split_a_range_at_month_starts(
    first: date, end: date, pieces: list[tuple[date, date]]
) -> None:
    assert restore._months(first, end) == pieces


def test_raw_restore_starts_two_days_before_the_newest_hour() -> None:
    newest = int(datetime(2026, 10, 10, 9, tzinfo=UTC).timestamp())
    assert restore._raw_from(newest, datetime(2026, 10, 10, 11, tzinfo=UTC)) == date(2026, 10, 9)


def test_raw_restore_never_starts_on_a_day_the_ttl_is_about_to_drop() -> None:
    # The archive's newest hour is a week old: the day the raw TTL drops next is skipped.
    newest = int(datetime(2026, 10, 3, 9, tzinfo=UTC).timestamp())
    now = datetime(2026, 10, 10, 11, tzinfo=UTC)
    assert restore._raw_from(newest, now) == date(2026, 10, 4)


class RawRange(StubClickHouse):
    def __init__(self, oldest: datetime | None) -> None:
        super().__init__()
        self.oldest = oldest
        self.windows: list[Mapping[str, Any]] = []

    def answer(self, sql: str, params: Mapping[str, Any]) -> QueryResult:
        if "oldest_s" in sql:
            n = 0 if self.oldest is None else 1
            oldest_s = 0 if self.oldest is None else int(self.oldest.timestamp())
            return QueryResult([{"oldest_s": oldest_s, "n": n}], NO_STATS)
        self.windows.append(params)
        return QueryResult([], NO_STATS)


NOW = datetime(2026, 10, 10, 12, 0, 30, tzinfo=UTC)


async def test_mismatches_are_looked_for_in_whole_settled_minutes() -> None:
    db = RawRange(oldest=datetime(2026, 10, 10, 11, 0, 20, tzinfo=UTC))
    await reconcile.find_mismatches(db, now=NOW, settle=timedelta(minutes=15))
    # From the first whole minute after the oldest row (it may be cut by the TTL), to the
    # minute the settle window starts.
    assert db.windows == [
        {
            "from_s": int(datetime(2026, 10, 10, 11, 1, tzinfo=UTC).timestamp()),
            "to_s": int(datetime(2026, 10, 10, 11, 45, tzinfo=UTC).timestamp()),
        }
    ]


@pytest.mark.parametrize("oldest", [None, datetime(2026, 10, 10, 11, 50, tzinfo=UTC)])
async def test_nothing_settled_means_nothing_to_compare(oldest: datetime | None) -> None:
    db = RawRange(oldest=oldest)
    assert await reconcile.find_mismatches(db, now=NOW, settle=timedelta(minutes=15)) == []
    assert db.windows == []


def test_the_lock_message_names_the_holder_and_the_way_out() -> None:
    message = lock.held_message("ip-10-20-1-5 at 2026-10-10T11:28:43+00:00")
    assert "ip-10-20-1-5 at 2026-10-10T11:28:43+00:00" in message
    assert "livedemos-migrate --unlock" in message
