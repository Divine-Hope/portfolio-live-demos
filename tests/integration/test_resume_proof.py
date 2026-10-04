"""The resume proof: kill ingest mid-stream, bring it back, lose nothing, duplicate nothing.

Runs the real ingest process against the fake EventStreams server (which drops every
connection after a few seconds, like the real one does) and SIGKILLs it while it's
writing. The fake server generated every event, so it knows exactly what should be in
the table. We compare against that ground truth, not against "looks about right".
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from collections.abc import AsyncIterator

import httpx
import pytest

from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import ClickHouseSettings, IngestSettings
from livedemos.ingest.consumer import Consumer

from .conftest import TEST_DB, clickhouse_test_settings, free_port

pytestmark = pytest.mark.integration

# Late events up to 5 minutes, like the real stream: the seam can't be found by event time.
FAKE_ARGS = ["--rate", "80", "--seed", "11", "--drop-after-s", "4", "--disorder-s", "300"]

# Every minute between the first and last event should have at least one row.
MINUTE_COVERAGE_SQL = """
SELECT
    uniqExact(toStartOfMinute(event_time)) AS present,
    dateDiff('minute', toStartOfMinute(min(event_time)), toStartOfMinute(max(event_time))) + 1
        AS span
FROM wiki_edits
"""


@pytest.fixture
async def fake_stream() -> AsyncIterator[str]:
    port = free_port()
    module = "livedemos.devtools.fake_eventstreams"
    cmd = [sys.executable, "-m", module, "--host", "127.0.0.1", "--port", str(port), *FAKE_ARGS]
    proc = subprocess.Popen(cmd)
    base = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient(timeout=2) as http:
        deadline = time.monotonic() + 15
        while True:
            try:
                if (await http.get(f"{base}/_control/truth")).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                pytest.fail("fake stream didn't start")
            await asyncio.sleep(0.2)
    yield base
    proc.terminate()
    proc.wait(timeout=10)


def start_ingest(stream_base: str) -> subprocess.Popen[bytes]:
    settings = clickhouse_test_settings()
    env = {
        **os.environ,
        "CLICKHOUSE_URL": settings.url,
        "CLICKHOUSE_USER": settings.user,
        "CLICKHOUSE_PASSWORD": settings.password,
        "CLICKHOUSE_DATABASE": TEST_DB,
        "INGEST_STREAM_URL": f"{stream_base}/v2/stream/recentchange",
        "INGEST_METRICS_PORT": str(free_port()),
        "INGEST_BACKOFF_INITIAL_S": "0.2",
        "INGEST_BACKOFF_MAX_S": "1",
    }
    return subprocess.Popen([sys.executable, "-m", "livedemos.ingest"], env=env)


async def test_kill_mid_stream_loses_nothing_and_duplicates_nothing(
    ch: ClickHouse, fake_stream: str
) -> None:
    first = start_ingest(fake_stream)
    await _wait_for_rows(ch, at_least=200)
    await asyncio.sleep(2.5)  # mid-batch, mid-connection
    first.send_signal(signal.SIGKILL)
    first.wait(timeout=10)
    rows_at_kill = await _scalar(ch, "SELECT count() FROM wiki_edits")

    await asyncio.sleep(4)  # the stream keeps moving while we're down

    second = start_ingest(fake_stream)
    try:
        await asyncio.sleep(6)  # catch up, then a couple of normal drop-and-reconnect cycles
        async with httpx.AsyncClient(timeout=5) as http:
            await http.post(f"{fake_stream}/_control/pause")
            truth = (await http.get(f"{fake_stream}/_control/truth")).json()
        expected = set(truth["event_ids"])

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if await _scalar(ch, "SELECT count() FROM wiki_edits") >= len(expected):
                break
            await asyncio.sleep(0.5)

        stored = await ch.query("SELECT toString(event_id) AS id FROM wiki_edits")
        stored_ids = [r["id"] for r in stored.rows]
        missing = expected - set(stored_ids)
        unexpected = set(stored_ids) - expected

        assert rows_at_kill > 0
        assert len(stored_ids) == len(set(stored_ids)), "duplicates in the raw table"
        assert not missing, f"{len(missing)} events never reached the table"
        assert not unexpected, f"{len(unexpected)} rows that the source never sent"
        rolled = await _scalar(ch, "SELECT sum(edits) FROM wiki_edits_per_minute")
        assert rolled == len(expected), "rollup disagrees with raw"
        coverage = (await ch.query(MINUTE_COVERAGE_SQL)).rows[0]
        assert coverage["present"] == coverage["span"], "a minute with no data inside the run"
    finally:
        second.send_signal(signal.SIGTERM)
        second.wait(timeout=15)


class CommitsThenFails(ClickHouse):
    """The nth insert lands in the table, then reports a timeout anyway.

    That's what a slow host does: the server commits, the client gives up waiting.
    """

    def __init__(self, settings: ClickHouseSettings, *, fail_on_call: int):
        super().__init__(settings)
        self.calls = 0
        self.fail_on_call = fail_on_call

    async def insert(self, table: str, rows: object, *, dedup_token: str | None = None) -> None:
        await super().insert(table, rows, dedup_token=dedup_token)  # type: ignore[arg-type]
        self.calls += 1
        if self.calls == self.fail_on_call:
            raise ClickHouseError("read timeout (the insert committed anyway)")


async def test_an_insert_that_commits_but_reports_failure_is_not_written_twice(
    ch: ClickHouse, fake_stream: str
) -> None:
    settings = IngestSettings(
        stream_url=f"{fake_stream}/v2/stream/recentchange",
        backoff_initial_s=0.1,
        backoff_max_s=0.5,
    )
    flaky = CommitsThenFails(clickhouse_test_settings(), fail_on_call=3)
    stop = asyncio.Event()
    consumer = asyncio.create_task(Consumer(settings, flaky).run(stop))
    try:
        await asyncio.sleep(8)
        async with httpx.AsyncClient(timeout=5) as http:
            await http.post(f"{fake_stream}/_control/pause")
            expected = set((await http.get(f"{fake_stream}/_control/truth")).json()["event_ids"])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if await _scalar(ch, "SELECT count() FROM wiki_edits") >= len(expected):
                break
            await asyncio.sleep(0.5)
        await asyncio.sleep(2)  # anything extra would have landed by now
    finally:
        stop.set()
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await flaky.aclose()

    stored = [
        r["id"] for r in (await ch.query("SELECT toString(event_id) AS id FROM wiki_edits")).rows
    ]
    assert flaky.calls >= 3, "the failure was never injected"
    assert len(stored) == len(set(stored)), "the committed-but-failed batch was written twice"
    assert set(stored) == expected


async def _scalar(ch: ClickHouse, sql: str) -> int:
    result = await ch.query(sql)
    return int(next(iter(result.rows[0].values())))


async def _wait_for_rows(ch: ClickHouse, *, at_least: int, timeout_s: float = 30) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await _scalar(ch, "SELECT count() FROM wiki_edits") >= at_least:
            return
        await asyncio.sleep(0.25)
    pytest.fail(f"ingest didn't write {at_least} rows in {timeout_s}s")
