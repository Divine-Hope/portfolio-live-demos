"""The freshness SLO maths (ops/slo.py), including minutes with no sample."""

import pytest

from livedemos.ops import slo

DAY = 86_400
NOW = 29_858_333 * 60 + 30  # 30 s into a minute
MINUTE_NOW = NOW - 30


def test_no_window_before_the_first_sample() -> None:
    assert slo.window(now_s=NOW, first_sample_s=None, days=30) is None


def test_the_window_is_30_days_of_whole_minutes_ending_before_the_current_one() -> None:
    win = slo.window(now_s=NOW, first_sample_s=MINUTE_NOW - 90 * DAY, days=30)
    assert win == slo.Window(start_s=MINUTE_NOW - 30 * DAY, end_s=MINUTE_NOW)
    assert win.minutes == 30 * 1_440
    assert win.is_full(30)


def test_the_window_starts_at_the_first_whole_minute_measured() -> None:
    # The first sample came 17 s into a minute: that minute wasn't measured from its start,
    # so counting begins with the next. The 30 days before don't count either way.
    win = slo.window(now_s=NOW, first_sample_s=MINUTE_NOW - 7_200 + 17, days=30)
    assert win is not None
    assert win.start_s == MINUTE_NOW - 7_200 + 60
    assert win.minutes == 119
    assert not win.is_full(30)


def test_a_sample_on_the_minute_counts_that_minute() -> None:
    win = slo.window(now_s=NOW, first_sample_s=float(MINUTE_NOW - 600), days=30)
    assert win is not None
    assert win.minutes == 10


def test_a_first_sample_in_the_current_minute_gives_an_empty_window() -> None:
    win = slo.window(now_s=NOW, first_sample_s=MINUTE_NOW + 5, days=30)
    assert win is not None
    assert win.minutes == 0
    report = slo.summarise(win=win, sampled=0, fresh=0, target=0.99, days=30)
    assert report.ratio is None
    assert report.met is None


def test_minutes_with_no_sample_count_against_the_slo() -> None:
    win = slo.Window(start_s=0, end_s=100 * 60)
    # 90 minutes sampled, 88 of them fresh; 10 minutes have no sample at all.
    report = slo.summarise(win=win, sampled=90, fresh=88, target=0.99, days=30)
    assert (report.fresh, report.stale, report.unmeasured) == (88, 2, 10)
    assert report.ratio == pytest.approx(0.88)
    assert report.met is False
    assert report.budget_used == 12


def test_a_window_with_no_samples_at_all_is_zero_not_unknown() -> None:
    # ClickHouse was down the whole time: nothing shows the data was fresh.
    win = slo.Window(start_s=0, end_s=60 * 60)
    report = slo.summarise(win=win, sampled=0, fresh=0, target=0.99, days=30)
    assert report.unmeasured == 60
    assert report.ratio == 0.0
    assert report.met is False


def test_the_target_is_inclusive() -> None:
    win = slo.Window(start_s=0, end_s=1_000 * 60)
    assert slo.summarise(win=win, sampled=1_000, fresh=990, target=0.99, days=30).met is True
    assert slo.summarise(win=win, sampled=1_000, fresh=989, target=0.99, days=30).met is False


def test_counts_outside_the_window_cant_push_the_ratio_over_one() -> None:
    win = slo.Window(start_s=0, end_s=10 * 60)
    report = slo.summarise(win=win, sampled=12, fresh=12, target=0.99, days=30)
    assert (report.fresh, report.stale, report.unmeasured) == (10, 0, 0)
    assert report.ratio == 1.0


def test_the_error_budget_is_one_percent_of_30_days() -> None:
    win = slo.Window(start_s=0, end_s=60)
    assert slo.summarise(win=win, sampled=1, fresh=1, target=0.99, days=30).budget_minutes == 432
