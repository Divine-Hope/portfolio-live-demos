"""The freshness SLO maths (ops/slo.py), including minutes with no sample."""

from datetime import UTC, datetime

import pytest

from livedemos.ops import slo

DAY = 86_400
NOW = datetime(2026, 10, 29, 14, 20, 30, tzinfo=UTC).timestamp()  # 30 s into a minute
MINUTE_NOW = int(NOW) - 30
OCTOBER = int(datetime(2026, 10, 1, tzinfo=UTC).timestamp())


def window(start_s: int, end_s: int) -> slo.Window:
    return slo.Window(start_s=start_s, end_s=end_s, month_s=start_s, month_days=30)


def test_no_window_before_the_first_sample() -> None:
    assert slo.window(now_s=NOW, first_sample_s=None) is None


def test_the_window_is_this_month_in_whole_minutes_ending_before_the_current_one() -> None:
    win = slo.window(now_s=NOW, first_sample_s=MINUTE_NOW - 90 * DAY)
    assert win == slo.Window(start_s=OCTOBER, end_s=MINUTE_NOW, month_s=OCTOBER, month_days=31)
    assert win.minutes == (MINUTE_NOW - OCTOBER) // 60
    assert win.is_full


def test_last_months_minutes_dont_count() -> None:
    # Half a minute into November: no whole minute of it yet, whatever October was like.
    now = datetime(2026, 11, 1, 0, 0, 30, tzinfo=UTC).timestamp()
    win = slo.window(now_s=now, first_sample_s=MINUTE_NOW - 90 * DAY)
    assert win is not None
    assert (win.minutes, win.month_days) == (0, 30)
    assert win.is_full
    report = slo.summarise(win=win, sampled=0, fresh=0, target=0.999)
    assert report.ratio is None
    assert report.budget_minutes == 43


def test_the_window_starts_at_the_first_whole_minute_measured() -> None:
    # The first sample came 17 s into a minute: that minute wasn't measured from its start,
    # so counting begins with the next. The month before it doesn't count either way.
    win = slo.window(now_s=NOW, first_sample_s=MINUTE_NOW - 7_200 + 17)
    assert win is not None
    assert win.start_s == MINUTE_NOW - 7_200 + 60
    assert win.minutes == 119
    assert not win.is_full


def test_a_sample_on_the_minute_counts_that_minute() -> None:
    win = slo.window(now_s=NOW, first_sample_s=float(MINUTE_NOW - 600))
    assert win is not None
    assert win.minutes == 10


def test_a_first_sample_in_the_current_minute_gives_an_empty_window() -> None:
    win = slo.window(now_s=NOW, first_sample_s=MINUTE_NOW + 5)
    assert win is not None
    assert win.minutes == 0
    report = slo.summarise(win=win, sampled=0, fresh=0, target=0.99)
    assert report.ratio is None
    assert report.met is None


def test_minutes_with_no_sample_count_against_the_slo() -> None:
    win = window(0, 100 * 60)
    # 90 minutes sampled, 88 of them fresh; 10 minutes have no sample at all.
    report = slo.summarise(win=win, sampled=90, fresh=88, target=0.99)
    assert (report.fresh, report.stale, report.unmeasured) == (88, 2, 10)
    assert report.ratio == pytest.approx(0.88)
    assert report.met is False
    assert report.budget_used == 12


def test_a_window_with_no_samples_at_all_is_zero_not_unknown() -> None:
    # ClickHouse was down the whole time: nothing shows the data was fresh.
    win = window(0, 60 * 60)
    report = slo.summarise(win=win, sampled=0, fresh=0, target=0.99)
    assert report.unmeasured == 60
    assert report.ratio == 0.0
    assert report.met is False


def test_the_target_is_inclusive() -> None:
    win = window(0, 1_000 * 60)
    assert slo.summarise(win=win, sampled=1_000, fresh=990, target=0.99).met is True
    assert slo.summarise(win=win, sampled=1_000, fresh=989, target=0.99).met is False


def test_counts_outside_the_window_cant_push_the_ratio_over_one() -> None:
    win = window(0, 10 * 60)
    report = slo.summarise(win=win, sampled=12, fresh=12, target=0.99)
    assert (report.fresh, report.stale, report.unmeasured) == (10, 0, 0)
    assert report.ratio == 1.0


@pytest.mark.parametrize(
    ("days", "target", "budget"),
    [
        (30, 0.99, 432),
        # 99.9%, the requirement: 40 to 45 minutes, by the month's length.
        (28, 0.999, 40),
        (29, 0.999, 42),
        (30, 0.999, 43),
        (31, 0.999, 45),
    ],
)
def test_the_error_budget_is_what_the_target_leaves_of_the_whole_month(
    days: int, target: float, budget: int
) -> None:
    win = slo.Window(start_s=0, end_s=60, month_s=0, month_days=days)
    assert slo.summarise(win=win, sampled=1, fresh=1, target=target).budget_minutes == budget


def test_the_month_is_the_utc_one() -> None:
    # 23:30 on 31 Dec UTC is still December, wherever the host is.
    now = datetime(2026, 12, 31, 23, 30, tzinfo=UTC).timestamp()
    assert slo.month(now) == (int(datetime(2026, 12, 1, tzinfo=UTC).timestamp()), 31)
    assert slo.month(datetime(2028, 2, 10, tzinfo=UTC).timestamp())[1] == 29
