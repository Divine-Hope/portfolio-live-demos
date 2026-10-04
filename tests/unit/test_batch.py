from dataclasses import replace
from datetime import UTC, datetime

import pytest

from livedemos.ingest.batch import Batch, RecentIds, SequenceGenerator
from livedemos.ingest.consumer import Backoff
from livedemos.ingest.events import Edit


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def edit(n: int) -> Edit:
    return Edit(
        event_id=f"00000000-0000-4000-8000-{n:012d}",
        event_time=datetime(2026, 10, 4, 18, 0, n % 60, tzinfo=UTC),
        wiki="enwiki",
        lang="en",
        type="edit",
        namespace=0,
        title=f"Page {n}",
        is_bot=False,
    )


def test_batch_is_due_after_the_interval() -> None:
    clock = FakeClock()
    batch = Batch(max_rows=100, interval_s=1.0, clock=clock)
    assert not batch.due()  # empty batches are never due
    batch.add(edit(1), sse_id="a", ingest_seq=1)
    assert not batch.due()
    clock.now += 1.0
    assert batch.due()


def test_batch_is_due_at_the_row_cap() -> None:
    batch = Batch(max_rows=3, interval_s=60.0, clock=FakeClock())
    for n in range(3):
        batch.add(edit(n), sse_id=str(n), ingest_seq=n)
    assert batch.due()


def test_events_sharing_a_millisecond_get_different_tokens() -> None:
    # SSE ids are timestamps: two different events can carry the same one.
    a = Batch(max_rows=10, interval_s=1.0, clock=FakeClock())
    b = Batch(max_rows=10, interval_s=1.0, clock=FakeClock())
    a.add(edit(1), sse_id="same-ms", ingest_seq=1)
    b.add(edit(2), sse_id="same-ms", ingest_seq=2)
    assert a.dedup_token() != b.dedup_token()


def test_a_batch_never_spans_two_days() -> None:
    batch = Batch(max_rows=10, interval_s=1.0, clock=FakeClock())
    before_midnight = edit(1)
    after_midnight = replace(
        before_midnight,
        event_id="00000000-0000-4000-8000-000000000099",
        event_time=datetime(2026, 10, 5, 0, 0, 1, tzinfo=UTC),
    )
    batch.add(before_midnight, sse_id="a", ingest_seq=1)
    assert not batch.accepts(after_midnight)
    with pytest.raises(ValueError, match="two partitions"):
        batch.add(after_midnight, sse_id="b", ingest_seq=2)


def test_bookmark_is_the_last_sse_id_and_token_is_stable() -> None:
    a = Batch(max_rows=10, interval_s=1.0, clock=FakeClock())
    b = Batch(max_rows=10, interval_s=1.0, clock=FakeClock())
    for batch in (a, b):
        batch.add(edit(1), sse_id="first", ingest_seq=1)
        batch.add(edit(2), sse_id="last", ingest_seq=2)
    assert a.last_sse_id == "last"
    assert a.dedup_token() == b.dedup_token()
    assert edit(1).event_id in a
    assert edit(3).event_id not in a


def test_recent_ids_keep_the_latest_ingested_whatever_their_timestamps() -> None:
    recent = RecentIds(max_ids=3)
    recent.extend(["a", "b", "c"])
    recent.extend(["d"])  # pushes out the oldest ingested, not the oldest event
    assert "a" not in recent
    assert {"b", "c", "d"} <= {i for i in "bcd" if i in recent}
    recent.extend(["c"])  # already known: no change
    assert len(recent) == 3


def test_sequence_never_goes_backwards() -> None:
    ticks = iter([1_000, 2_000, 1_500, 1_500])
    seq = SequenceGenerator(clock=lambda: next(ticks))
    values = [seq.next() for _ in range(4)]
    assert values == sorted(values)
    assert len(set(values)) == 4


def test_backoff_stays_under_the_cap_and_resets() -> None:
    backoff = Backoff(initial_s=1.0, max_s=8.0)
    delays = [backoff.next_delay() for _ in range(10)]
    assert all(0 <= d <= 8.0 for d in delays)
    backoff.reset()
    assert backoff.next_delay() <= 1.0
