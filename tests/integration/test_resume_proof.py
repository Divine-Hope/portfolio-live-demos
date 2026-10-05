"""The resume proof: kill ingest mid-insert, bring it back, lose nothing, duplicate nothing.

Runs the real ingest process against the fake EventStreams server, which drops every
connection after a few seconds like the real one, interleaves two topics whose positions
advance independently, repeats milliseconds, and delivers late events. A test-only
materialized view makes every insert take ~1.5 s on the server, so the SIGKILL can be
timed to land while an insert is provably running (it's in system.processes), not just
"probably mid-batch". The fake server generated every event, so it knows exactly what
should be in the table: we compare against that ground truth, per minute and language.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import ClickHouseSettings, IngestSettings
from livedemos.ingest.consumer import Consumer
from livedemos.reconcile import find_mismatches

from .conftest import TEST_DB, clickhouse_test_settings, free_port

pytestmark = pytest.mark.integration

# Late events up to 5 minutes, like the real stream: the seam can't be found by event time.
# Two topics 1.5 s apart, and a fifth of ids sharing a millisecond with the one before.
FAKE_ARGS = [
    "--rate", "80", "--seed", "11", "--drop-after-s", "4", "--disorder-s", "300",
    "--topic-skew-ms", "1500", "--same-ms-share", "0.2",
]  # fmt: skip

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


async def test_kill_mid_insert_loses_nothing_and_duplicates_nothing(
    ch: ClickHouse, fake_stream: str
) -> None:
    await _make_inserts_slow(ch)
    first = start_ingest(fake_stream)
    await _wait_for_rows(ch, at_least=200)
    await _wait_for_insert_in_flight(ch)  # the barrier: an insert is running right now
    first.send_signal(signal.SIGKILL)
    first.wait(timeout=10)
    # The killed process's insert is still running on the server. Start the replacement
    # now, while it is: reading the bookmark before it lands is the race being tested.
    orphaned = await _scalar(
        ch,
        "SELECT count() FROM system.processes "
        "WHERE startsWith(query_id, 'ingest-') AND current_database = currentDatabase()",
    )
    second = start_ingest(fake_stream)
    try:
        await asyncio.sleep(6)  # catch up, then a couple of normal drop-and-reconnect cycles
        expected = await _pause_and_get_truth(fake_stream)
        await _wait_for_count(ch, len(expected))
        await _assert_exactly(ch, expected)
        assert orphaned, "the killed process's insert had already finished; nothing raced"
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

    async def insert(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        dedup_token: str | None = None,
        query_id: str | None = None,
    ) -> None:
        await super().insert(table, rows, dedup_token=dedup_token, query_id=query_id)
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
        expected = await _pause_and_get_truth(fake_stream)
        await _wait_for_count(ch, len(expected))
        await asyncio.sleep(2)  # anything extra would have landed by now
    finally:
        stop.set()
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await flaky.aclose()
    assert flaky.calls >= 3, "the failure was never injected"
    await _assert_exactly(ch, expected)


class LandsLate(ClickHouse):
    """The nth insert fails fast on the client, but the server commits it a second later.

    The ambiguous case that reloading state can't fix: the commit lands after anything
    the client could read. Retrying the sealed batch with the same token is what makes
    the late copy a no-op.
    """

    def __init__(self, settings: ClickHouseSettings, *, fail_on_call: int):
        super().__init__(settings)
        self.calls = 0
        self.fail_on_call = fail_on_call
        self.late: asyncio.Task[None] | None = None

    async def insert(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        dedup_token: str | None = None,
        query_id: str | None = None,
    ) -> None:
        self.calls += 1
        if self.calls != self.fail_on_call:
            await super().insert(table, rows, dedup_token=dedup_token, query_id=query_id)
            return

        async def land_later() -> None:
            await asyncio.sleep(1.0)
            await super(LandsLate, self).insert(table, rows, dedup_token=dedup_token)

        self.late = asyncio.create_task(land_later())
        raise ClickHouseError("read timeout (the server will commit it anyway, later)")


async def test_an_insert_that_lands_after_its_retry_is_not_written_twice(
    ch: ClickHouse, fake_stream: str
) -> None:
    settings = IngestSettings(
        stream_url=f"{fake_stream}/v2/stream/recentchange",
        backoff_initial_s=0.1,
        backoff_max_s=0.3,
    )
    flaky = LandsLate(clickhouse_test_settings(), fail_on_call=3)
    stop = asyncio.Event()
    consumer = asyncio.create_task(Consumer(settings, flaky).run(stop))
    try:
        await asyncio.sleep(8)
        expected = await _pause_and_get_truth(fake_stream)
        await _wait_for_count(ch, len(expected))
        assert flaky.late is not None, "the failure was never injected"
        await flaky.late  # the late copy has landed (and been dropped as a duplicate)
        await asyncio.sleep(1)
    finally:
        stop.set()
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await flaky.aclose()
    await _assert_exactly(ch, expected)


async def _make_inserts_slow(ch: ClickHouse) -> None:
    """Test-only: every insert into wiki_edits spends ~1.5 s in this view, server-side."""
    await ch.execute("CREATE TABLE slow_sink (z UInt8) ENGINE = Null")
    await ch.execute(
        "CREATE MATERIALIZED VIEW slow_mv TO slow_sink AS SELECT sleep(1.5) AS z FROM wiki_edits"
    )


async def _wait_for_insert_in_flight(ch: ClickHouse, timeout_s: float = 30) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        running = await _scalar(
            ch,
            "SELECT count() FROM system.processes "
            "WHERE startsWith(query_id, 'ingest-') AND current_database = currentDatabase()",
        )
        if running:
            return
        await asyncio.sleep(0.02)
    pytest.fail("never saw an ingest insert running")


async def _pause_and_get_truth(fake_stream: str) -> set[str]:
    async with httpx.AsyncClient(timeout=5) as http:
        await http.post(f"{fake_stream}/_control/pause")
        truth = (await http.get(f"{fake_stream}/_control/truth")).json()
    return set(truth["event_ids"])


async def _wait_for_count(ch: ClickHouse, n: int, timeout_s: float = 30) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await _scalar(ch, "SELECT count() FROM wiki_edits") >= n:
            return
        await asyncio.sleep(0.5)


async def _assert_exactly(ch: ClickHouse, expected: set[str]) -> None:
    stored = await ch.query("SELECT toString(event_id) AS id FROM wiki_edits")
    stored_ids = [r["id"] for r in stored.rows]
    missing = expected - set(stored_ids)
    unexpected = set(stored_ids) - expected
    assert len(stored_ids) == len(set(stored_ids)), "duplicates in the raw table"
    assert not missing, f"{len(missing)} events never reached the table"
    assert not unexpected, f"{len(unexpected)} rows that the source never sent"
    rolled = await _scalar(ch, "SELECT sum(edits) FROM wiki_edits_per_minute")
    assert rolled == len(expected), "rollup total disagrees with raw"
    # Per minute and language, not only in total: drift can cancel out in a sum.
    now = datetime.now(UTC) + timedelta(minutes=1)
    assert await find_mismatches(ch, now=now, settle=timedelta(0)) == []
    coverage = (await ch.query(MINUTE_COVERAGE_SQL)).rows[0]
    assert coverage["present"] == coverage["span"], "a minute with no data inside the run"


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
