"""Small asyncio helpers shared by the services."""

from __future__ import annotations

import asyncio
import contextlib


async def sleep_unless_stopped(stop: asyncio.Event, seconds: float) -> None:
    """Sleep for `seconds`, or until `stop` is set, whichever comes first."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=max(0.0, seconds))
