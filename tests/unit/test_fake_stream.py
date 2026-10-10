import json

from devtools.fake_eventstreams import TOPICS, FakeStream


def filled(n: int = 200, *, topic_skew_ms: int = 1_500, same_ms_share: float = 0.2) -> FakeStream:
    fake = FakeStream(seed=7, topic_skew_ms=topic_skew_ms, same_ms_share=same_ms_share)
    for _ in range(n):
        fake.append(fake.make_event())
    return fake


def test_ids_carry_a_position_per_topic() -> None:
    fake = filled()
    positions = json.loads(fake.events[-1].sse_id)
    assert [p["topic"] for p in positions] == list(TOPICS)
    assert all("timestamp" in p for p in positions)
    # The first event's id can't have a timestamp for a topic nobody has written to yet.
    first = json.loads(fake.events[0].sse_id)
    assert any("offset" in p for p in first)


def test_topics_advance_independently() -> None:
    fake = filled(topic_skew_ms=5_000)
    eqiad, codfw = (json.loads(fake.events[-1].sse_id)[i]["timestamp"] for i in (0, 1))
    assert eqiad - codfw >= 4_000


def test_some_events_share_a_millisecond() -> None:
    fake = filled(same_ms_share=0.5)
    per_topic = [(e.topic, e.ts_ms) for e in fake.events]
    assert len(set(per_topic)) < len(per_topic)


def test_resume_is_inclusive_per_topic() -> None:
    fake = filled()
    bookmark = fake.events[100]
    replayed = fake.replay(fake.cursor(bookmark.sse_id, None))
    seqs = {e.seq for e in replayed}
    # Nothing after the bookmark is lost...
    assert all(e.seq in seqs for e in fake.events[101:])
    # ...the bookmarked event comes back, and so do earlier events that share a topic
    # position with it: the seam a resume has to deduplicate.
    assert bookmark.seq in seqs
    assert min(seqs) <= bookmark.seq


def test_no_bookmark_means_live_tail_and_since_rewinds() -> None:
    fake = filled()
    assert fake.replay(fake.cursor(None, None)) == []
    since = fake.events[10].ts_ms
    replayed = fake.replay(fake.cursor(None, str(since)))
    assert replayed
    assert all(e.ts_ms >= since for e in replayed)


def test_truth_counts_only_events_we_track() -> None:
    fake = filled(500)
    truth = fake.truth(0, 2**62)
    tracked = [e for e in fake.events if e.tracked]
    assert truth["tracked_events"] == len(tracked) > 0
    assert len(tracked) < len(fake.events)  # there's noise to filter out


def test_understands_real_ids_with_an_offset_entry() -> None:
    fake = filled()
    eqiad_events = [e for e in fake.events if e.topic == TOPICS[0]]
    real_shape = json.dumps(
        [
            {"topic": TOPICS[0], "partition": 0, "timestamp": eqiad_events[5].ts_ms},
            {"topic": TOPICS[1], "partition": 0, "offset": -1},
        ]
    )
    replayed = fake.replay(fake.cursor(real_shape, None))
    assert eqiad_events[5] in replayed
    assert all(e.topic == TOPICS[0] for e in replayed)  # codfw: tail only, nothing buffered
