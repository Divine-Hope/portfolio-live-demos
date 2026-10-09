"""The archive service only writes on the host holding the Elastic IP (ADR 0010)."""

import asyncio
from datetime import datetime
from typing import Any

import pytest

from livedemos.archive import __main__ as service


class CountingArchiver:
    def __init__(self) -> None:
        self.runs = 0

    async def run_once(self, now: datetime) -> list[Any]:
        self.runs += 1
        return []


@pytest.mark.parametrize(
    ("only_on_ip", "this_host", "runs"),
    [
        ("", None, True),
        ("203.0.113.7", "203.0.113.7", True),
        ("203.0.113.7", "198.51.100.1", False),
        ("203.0.113.7", None, False),
    ],
)
async def test_only_the_live_host_archives(
    monkeypatch: pytest.MonkeyPatch, only_on_ip: str, this_host: str | None, runs: bool
) -> None:
    async def fake_ip() -> str | None:
        return this_host

    monkeypatch.setattr(service, "public_ip", fake_ip)
    archiver = CountingArchiver()
    task = asyncio.create_task(service._serve(archiver, None, 3600, only_on_ip))  # type: ignore[arg-type]
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (archiver.runs > 0) is runs
