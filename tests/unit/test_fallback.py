import asyncio
import time
from typing import Any

import pytest

from livedemos.api.fallback import CACHE_CONTROL, KEY, FallbackWriter
from livedemos.api.snapshot import Snapshot


class StubSnapshotter:
    def __init__(self, latest: Snapshot | None):
        self.latest = latest


class StubS3:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.puts: list[dict[str, Any]] = []

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        if self.fail:
            raise ConnectionError("S3 is unreachable")
        self.puts.append(kwargs)
        return {}


def writer(latest: Snapshot | None, s3: StubS3) -> FallbackWriter:
    return FallbackWriter(
        StubSnapshotter(latest),  # type: ignore[arg-type]
        s3,
        bucket="snapshots",
        interval_s=60,
        max_snapshot_age_s=10,
        stale_after_s=60,
    )


def snapshot(*, built_ago_s: float = 0.5, last_event_age_s: float | None = 2.0) -> Snapshot:
    return Snapshot(
        body=b'{"status":"live"}',
        built_at=time.monotonic() - built_ago_s,
        last_event_age_s=last_event_age_s,
    )


async def test_writes_a_fresh_live_snapshot_as_is() -> None:
    s3 = StubS3()
    assert await writer(snapshot(), s3).write_once() == "written"
    assert s3.puts == [
        {
            "Bucket": "snapshots",
            "Key": KEY,
            "Body": b'{"status":"live"}',
            "ContentType": "application/json",
            "CacheControl": CACHE_CONTROL,
        }
    ]
    assert KEY == "v1/wikipedia/live.json"  # the path CloudFront asks the bucket for


@pytest.mark.parametrize(
    ("latest", "outcome"),
    [
        (None, "skipped_no_snapshot"),
        (snapshot(built_ago_s=30), "skipped_stale_snapshot"),  # ClickHouse is down
        (snapshot(last_event_age_s=600), "skipped_paused"),  # ingest has stopped
        (snapshot(last_event_age_s=None), "skipped_paused"),  # empty table
    ],
)
async def test_never_replaces_good_data_with_worse(latest: Snapshot | None, outcome: str) -> None:
    s3 = StubS3()
    assert await writer(latest, s3).write_once() == outcome
    assert s3.puts == []


async def test_s3_errors_are_counted_not_raised() -> None:
    assert await writer(snapshot(), StubS3(fail=True)).write_once() == "error"


async def test_run_stops_promptly() -> None:
    s3 = StubS3()
    stop = asyncio.Event()
    task = asyncio.create_task(writer(snapshot(), s3).run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert len(s3.puts) == 1
