"""The freshness SLO: the share of minutes whose newest event was under a minute old.

ClickHouse samples the newest event's age once a minute (migration 0005). Over a window
of whole minutes, each minute is one of:

- fresh: sampled, and every sample in it was under the threshold,
- stale: sampled, and some sample was over it (or there were no rows at all),
- unmeasured: no sample. ClickHouse was down, or the host was being replaced. We can't
  show it was fresh, so it counts against the SLO like a stale one.

The window is the current UTC calendar month to date, so a bad month doesn't weigh on
the next one. It never starts before measuring did: it starts at the first whole minute
after the first sample, so a minute measured for only its last second isn't counted. The
current minute is left out because its sample may not have landed yet. The error budget
is for the whole month, so it's the same on the 2nd as on the 30th.
"""

from __future__ import annotations

import calendar
import math
from dataclasses import dataclass
from datetime import UTC, datetime

MINUTE_S = 60


@dataclass(frozen=True, slots=True)
class Window:
    start_s: int  # first minute, inclusive
    end_s: int  # exclusive: the start of the current minute
    month_s: int  # the start of the month it's in
    month_days: int

    @property
    def minutes(self) -> int:
        return (self.end_s - self.start_s) // MINUTE_S

    @property
    def is_full(self) -> bool:
        """Whether measuring started before the month did: every minute so far counts."""
        return self.start_s <= self.month_s


@dataclass(frozen=True, slots=True)
class Freshness:
    window: Window
    fresh: int
    stale: int
    unmeasured: int
    ratio: float | None  # None while no whole minute has been measured
    met: bool | None
    budget_minutes: int  # minutes allowed to miss over the whole month
    budget_used: int


def month(now_s: float) -> tuple[int, int]:
    """(the start of the current UTC month, its length in days)."""
    now = datetime.fromtimestamp(now_s, UTC)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp()), calendar.monthrange(now.year, now.month)[1]


def budget_minutes(*, days: int, target: float) -> int:
    """Minutes allowed to miss the threshold over a month of `days` days."""
    return round(days * 1_440 * (1 - target))


def window(*, now_s: float, first_sample_s: float | None) -> Window | None:
    """The minutes to count, or None before the first sample."""
    if first_sample_s is None:
        return None
    month_s, month_days = month(now_s)
    end = int(now_s) // MINUTE_S * MINUTE_S
    first_whole = math.ceil(first_sample_s / MINUTE_S) * MINUTE_S
    start = max(month_s, first_whole)
    return Window(start_s=min(start, end), end_s=end, month_s=month_s, month_days=month_days)


def summarise(*, win: Window, sampled: int, fresh: int, target: float) -> Freshness:
    """Classify the window's minutes from the counts ClickHouse returns for it.

    `sampled` is the number of distinct minutes with a sample, `fresh` how many of those
    were fresh. Both are clamped to the window, so a stray duplicate can't push the ratio
    over 100%.
    """
    minutes = win.minutes
    sampled = max(0, min(sampled, minutes))
    fresh = max(0, min(fresh, sampled))
    stale = sampled - fresh
    unmeasured = minutes - sampled
    ratio = fresh / minutes if minutes else None
    return Freshness(
        window=win,
        fresh=fresh,
        stale=stale,
        unmeasured=unmeasured,
        ratio=ratio,
        met=None if ratio is None else ratio >= target,
        budget_minutes=budget_minutes(days=win.month_days, target=target),
        budget_used=stale + unmeasured,
    )
