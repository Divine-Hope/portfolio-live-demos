"""The freshness SLO: the share of minutes whose newest event was under a minute old.

ClickHouse samples the newest event's age once a minute (migration 0005). Over a window
of whole minutes, each minute is one of:

- fresh: sampled, and every sample in it was under the threshold,
- stale: sampled, and some sample was over it (or there were no rows at all),
- unmeasured: no sample. ClickHouse was down, or the host was being replaced. We can't
  show it was fresh, so it counts against the SLO like a stale one.

The window is the last `days` days of completed minutes, but never starts before
measuring did: it starts at the first whole minute after the first sample, so a minute
measured for only its last second isn't counted. The current minute is left out because
its sample may not have landed yet.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

MINUTE_S = 60


@dataclass(frozen=True, slots=True)
class Window:
    start_s: int  # first minute, inclusive
    end_s: int  # exclusive: the start of the current minute

    @property
    def minutes(self) -> int:
        return (self.end_s - self.start_s) // MINUTE_S

    def is_full(self, days: int) -> bool:
        """Whether measuring started before the window did: the whole `days` count."""
        return self.minutes >= days * 1_440


@dataclass(frozen=True, slots=True)
class Freshness:
    window: Window
    fresh: int
    stale: int
    unmeasured: int
    ratio: float | None  # None while no whole minute has been measured
    met: bool | None
    budget_minutes: int  # minutes allowed to miss over a full window
    budget_used: int


def window(*, now_s: float, first_sample_s: float | None, days: int) -> Window | None:
    """The minutes to count, or None before the first sample."""
    if first_sample_s is None:
        return None
    end = int(now_s) // MINUTE_S * MINUTE_S
    first_whole = math.ceil(first_sample_s / MINUTE_S) * MINUTE_S
    start = max(end - days * 86_400, first_whole)
    return Window(start_s=min(start, end), end_s=end)


def summarise(*, win: Window, sampled: int, fresh: int, target: float, days: int) -> Freshness:
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
        budget_minutes=round(days * 1_440 * (1 - target)),
        budget_used=stale + unmeasured,
    )
