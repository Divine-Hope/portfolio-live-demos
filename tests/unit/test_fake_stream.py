import json

from livedemos.devtools.fake_eventstreams import FakeStream


def filled(n: int = 50) -> FakeStream:
    fake = FakeStream(seed=7)
    for _ in range(n):
        stored = fake.make_event()
        fake.events.append(stored)
        fake.ts.append(stored.ts_ms)
    return fake


def test_ids_look_like_wikimedia_ids() -> None:
    fake = filled(1)
    positions = json.loads(fake.events[0].sse_id)
    assert {p["topic"] for p in positions} == {
        "eqiad.mediawiki.recentchange",
        "codfw.mediawiki.recentchange",
    }
    assert all("timestamp" in p for p in positions)


def test_resume_is_inclusive_like_the_real_service() -> None:
    fake = filled()
    bookmark = fake.events[20].sse_id
    assert fake.start_index(bookmark, None) == 20  # the bookmarked event comes back


def test_no_bookmark_means_live_tail_and_since_rewinds() -> None:
    fake = filled()
    assert fake.start_index(None, None) == len(fake.events)
    assert fake.start_index(None, str(fake.events[10].ts_ms)) == 10


def test_truth_counts_only_events_we_track() -> None:
    fake = filled(500)
    truth = fake.truth(0, 2**62)
    tracked = [e for e in fake.events if e.tracked]
    assert truth["tracked_events"] == len(tracked) > 0
    assert len(tracked) < len(fake.events)  # there's noise to filter out


def test_understands_real_ids_with_an_offset_entry() -> None:
    fake = filled()
    real_shape = json.dumps(
        [
            {"topic": "eqiad.mediawiki.recentchange", "partition": 0, "timestamp": fake.ts[5]},
            {"topic": "codfw.mediawiki.recentchange", "partition": 0, "offset": -1},
        ]
    )
    assert fake.start_index(real_shape, None) == 5
