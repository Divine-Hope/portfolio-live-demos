"""The consumer's failure handling, against a stub database (no network, no ClickHouse)."""

import asyncio
import json
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx_sse import ServerSentEvent

from livedemos.clickhouse import ClickHouseError, QueryResult, QueryStats
from livedemos.config import IngestSettings
from livedemos.ingest.batch import Batch
from livedemos.ingest.consumer import _EOF, Consumer, _read_into
from livedemos.ingest.events import Edit

SETTINGS = IngestSettings(backoff_initial_s=0.001, backoff_max_s=0.002)


def valid_event(n: int) -> str:
    return json.dumps(
        {
            "meta": {
                "id": f"00000000-0000-4000-8000-{n:012d}",
                "dt": datetime.now(UTC).isoformat(),
                "domain": "en.wikipedia.org",
            },
            "wiki": "enwiki",
            "type": "edit",
            "namespace": 0,
            "title": f"Page {n}",
            "bot": False,
        }
    )


class StubDatabase:
    def __init__(self, *, fail_inserts: int = 0) -> None:
        self.fail_inserts = fail_inserts
        self.inserts: list[tuple[str, str | None, str | None, int]] = []

    async def query(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> QueryResult:
        return QueryResult([], QueryStats(0.0, 0, 0))

    async def insert(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        dedup_token: str | None = None,
        query_id: str | None = None,
    ) -> None:
        self.inserts.append((table, dedup_token, query_id, len(rows)))
        if self.fail_inserts:
            self.fail_inserts -= 1
            raise ClickHouseError("read timeout (did it commit? nobody knows)")


class FakeSource:
    def __init__(self, messages: list[ServerSentEvent]) -> None:
        self._messages = messages

    async def aiter_sse(self) -> AsyncIterator[ServerSentEvent]:
        for message in self._messages:
            yield message


async def test_a_poison_message_doesnt_stop_the_reader() -> None:
    consumer = Consumer(SETTINGS, StubDatabase())
    messages = [
        ServerSentEvent(data="null", id="1"),
        ServerSentEvent(data="[1, 2]", id="2"),
        ServerSentEvent(data='{"wiki": ["enwiki"], "meta": {}}', id="3"),
        ServerSentEvent(data="{not json", id="4"),
        ServerSentEvent(data=valid_event(5), id="5"),
    ]
    queue: asyncio.Queue[Any] = asyncio.Queue()
    await _read_into(FakeSource(messages), queue, consumer._parse)  # type: ignore[arg-type]
    first, second = queue.get_nowait(), queue.get_nowait()
    assert isinstance(first, tuple)
    assert isinstance(first[0], Edit)
    assert first[1] == "5"
    assert second is _EOF
    assert queue.empty()


def test_events_the_restored_rollup_already_counts_are_dropped() -> None:
    consumer = Consumer(SETTINGS, StubDatabase())
    consumer._floor = datetime.now(UTC) + timedelta(seconds=30)
    assert consumer._parse(valid_event(1), "sse-1") is None
    consumer._floor = datetime.now(UTC) - timedelta(seconds=30)
    assert consumer._parse(valid_event(2), "sse-2") is not None


async def test_a_failed_insert_is_retried_unchanged_before_the_bookmark_moves() -> None:
    db = StubDatabase(fail_inserts=2)
    consumer = Consumer(SETTINGS, db)
    batch = Batch(max_rows=10, interval_s=1.0)
    for n in range(3):
        edit = consumer._parse(valid_event(n), f"sse-{n}")
        assert edit is not None
        batch.add(edit, sse_id=f"sse-{n}", ingest_seq=n + 1)

    with pytest.raises(ClickHouseError):
        await consumer._flush(batch)
    assert consumer._bookmark is None  # outcome unknown: the bookmark stays put
    assert consumer._pending is not None

    await consumer._commit_pending(asyncio.Event())
    assert consumer._bookmark == "sse-2"
    assert consumer._pending is None
    # Every attempt sent the same rows, with the same token and query id.
    assert len(db.inserts) == 3
    assert len({(token, qid, n) for _, token, qid, n in db.inserts}) == 1
    _, token, query_id, rows = db.inserts[0]
    assert query_id == f"ingest-{token}"
    assert rows == 3


async def test_stopping_mid_retry_leaves_the_bookmark_alone() -> None:
    db = StubDatabase(fail_inserts=1_000)
    consumer = Consumer(SETTINGS, db)
    batch = Batch(max_rows=10, interval_s=1.0)
    edit = consumer._parse(valid_event(1), "sse-1")
    assert edit is not None
    batch.add(edit, sse_id="sse-1", ingest_seq=1)
    consumer._pending = batch.seal()
    stop = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, stop.set)
    await consumer._commit_pending(stop)
    assert consumer._bookmark is None


async def test_a_bookmark_older_than_retention_is_dropped_with_a_gap() -> None:
    db = StubDatabase()
    consumer = Consumer(SETTINGS, db)
    consumer._bookmark = "old"
    consumer._bookmark_time = datetime.now(UTC) - timedelta(seconds=SETTINGS.retention_s + 60)
    await consumer._expire_old_bookmark()
    assert consumer._bookmark is None
    assert consumer._since is not None
    assert [table for table, *_ in db.inserts] == ["ingest_gaps"]


async def test_a_fresh_bookmark_is_kept() -> None:
    db = StubDatabase()
    consumer = Consumer(SETTINGS, db)
    consumer._bookmark = "recent"
    consumer._bookmark_time = datetime.now(UTC) - timedelta(minutes=5)
    await consumer._expire_old_bookmark()
    assert consumer._bookmark == "recent"
    assert db.inserts == []


class InsertStillRunning(StubDatabase):
    """system.processes shows a predecessor's insert for the first `polls` checks."""

    def __init__(self, polls: int) -> None:
        super().__init__()
        self.polls = polls
        self.log: list[str] = []

    async def query(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> QueryResult:
        if "system.processes" in sql:
            running = self.polls > 0
            self.polls -= 1
            self.log.append("running" if running else "done")
            return QueryResult([{"n": int(running)}], QueryStats(0.0, 0, 0))
        self.log.append("read")
        return QueryResult([{"n": 0, "newest_ms": 0, "newest_s": 0}], QueryStats(0.0, 0, 0))


async def test_start_reads_no_state_while_an_earlier_insert_runs() -> None:
    db = InsertStillRunning(polls=4)
    settings = SETTINGS.model_copy(update={"inflight_warn_s": 0.01})
    consumer = Consumer(settings, db)
    await asyncio.wait_for(consumer._load_state(asyncio.Event()), timeout=10)
    assert db.log.count("running") == 4
    first_read = db.log.index("read")
    assert "running" not in db.log[first_read:]  # no state read until it had finished
