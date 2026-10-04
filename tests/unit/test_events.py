import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from livedemos.ingest.events import Edit, Skip, lang_of, parse, to_row

WIKIS = frozenset({"enwiki", "ptwiki", "dewiki"})
TYPES = frozenset({"edit", "new"})


def event(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "meta": {
            "id": "6f7a3b2e-1c4d-4e5f-8a9b-0c1d2e3f4a5b",
            "dt": "2026-10-04T18:00:00.123Z",
            "domain": "en.wikipedia.org",
        },
        "type": "edit",
        "namespace": 0,
        "title": "Portugal",
        "bot": False,
        "wiki": "enwiki",
    }
    base.update(overrides)
    return base


def test_keeps_a_normal_edit() -> None:
    result = parse(event(), wikis=WIKIS, types=TYPES)
    assert isinstance(result, Edit)
    assert result.lang == "en"
    assert result.title == "Portugal"
    assert result.event_time == datetime(2026, 10, 4, 18, 0, 0, 123000, tzinfo=UTC)


def test_drops_canary_events() -> None:
    canary = event(
        meta={"id": "6f7a3b2e-1c4d-4e5f-8a9b-0c1d2e3f4a5b", "dt": "x", "domain": "canary"}
    )
    assert parse(canary, wikis=WIKIS, types=TYPES) is Skip.CANARY


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"wiki": "wikidatawiki"}, Skip.OTHER_WIKI),
        ({"type": "log"}, Skip.OTHER_TYPE),
        ({"type": "categorize"}, Skip.OTHER_TYPE),
        ({"wiki": None}, Skip.MALFORMED),
        ({"meta": "not a dict"}, Skip.MALFORMED),
        ({"title": ""}, Skip.MALFORMED),
        ({"namespace": "zero"}, Skip.MALFORMED),
    ],
)
def test_skips_with_a_reason(overrides: dict[str, object], reason: Skip) -> None:
    assert parse(event(**overrides), wikis=WIKIS, types=TYPES) is reason


def test_rejects_bad_ids_and_naive_times() -> None:
    bad_id = event(meta={"id": "nope", "dt": "2026-10-04T18:00:00Z", "domain": "x"})
    naive = event(
        meta={
            "id": "6f7a3b2e-1c4d-4e5f-8a9b-0c1d2e3f4a5b",
            "dt": "2026-10-04T18:00:00",
            "domain": "x",
        }
    )
    assert parse(bad_id, wikis=WIKIS, types=TYPES) is Skip.MALFORMED
    assert parse(naive, wikis=WIKIS, types=TYPES) is Skip.MALFORMED


def test_lang_of() -> None:
    assert lang_of("enwiki") == "en"
    assert lang_of("ptwiki") == "pt"


def test_row_times_are_utc_with_millis() -> None:
    edit = parse(event(), wikis=WIKIS, types=TYPES)
    assert isinstance(edit, Edit)
    plus_one = datetime(2026, 10, 4, 19, 0, 0, 500000, tzinfo=timezone(timedelta(hours=1)))
    row = to_row(edit, sse_id="[]", ingest_seq=7, ingested_at=plus_one)
    assert row["event_time"] == "2026-10-04T18:00:00.123Z"
    assert row["ingested_at"] == "2026-10-04T18:00:00.500Z"
    assert row["ingest_seq"] == 7
    assert row["sse_id"] == "[]"


def test_skips_events_dated_far_in_the_future() -> None:
    now = datetime(2026, 10, 4, 17, 0, tzinfo=UTC)
    # The default event is dated 18:00:00, an hour after `now`.
    assert parse(event(), wikis=WIKIS, types=TYPES, now=now) is Skip.FUTURE
    a_minute_before = datetime(2026, 10, 4, 17, 59, tzinfo=UTC)
    assert isinstance(parse(event(), wikis=WIKIS, types=TYPES, now=a_minute_before), Edit)


def test_a_real_captured_event() -> None:
    """An event captured from the live stream on 2026-10-04.

    Real field names and shapes; the user, comment and a few ids are replaced.
    """
    raw = json.loads(
        (Path(__file__).parents[1] / "fixtures" / "recentchange_enwiki_edit.json").read_text()
    )
    now = datetime(2026, 10, 4, 19, 47, tzinfo=UTC)
    result = parse(raw, wikis=WIKIS, types=TYPES, now=now)
    assert isinstance(result, Edit)
    assert (result.lang, result.namespace, result.is_bot) == ("en", 2, True)
    assert result.event_time == datetime(2026, 10, 4, 19, 46, 13, 321000, tzinfo=UTC)
