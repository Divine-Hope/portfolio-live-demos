from typing import Any

from livedemos.api.contract import article_url
from livedemos.api.snapshot import MINUTES, assemble

LANGS = ["en", "pt", "de"]
NEWEST = 1_759_600_000.0  # a fixed instant, minute-aligned + 40 s below
NEWEST_MINUTE = int(NEWEST // 60) * 60


def build(**overrides: Any) -> Any:
    args: dict[str, Any] = {
        "now": NEWEST + 2,
        "newest": NEWEST,
        "langs": LANGS,
        "window_rows": [
            {"lang": "en", "edits": 100, "pages": 80, "bot_edits": 20},
            {"lang": "pt", "edits": 10, "pages": 9, "bot_edits": 0},
        ],
        "top_rows": [
            {"lang": "en", "title": "Portugal", "edits": 7},
            {"lang": "en", "title": "Lisbon", "edits": 3},
            {"lang": "pt", "title": "Lisboa", "edits": 5},
        ],
        "minute_rows": [
            {"minute_s": NEWEST_MINUTE, "lang": "en", "edits": 30},
            {"minute_s": NEWEST_MINUTE - 60, "lang": "en", "edits": 40},
            {"minute_s": NEWEST_MINUTE - 60, "lang": "pt", "edits": 4},
        ],
        "lag": {"p50_ms": 812.0, "p95_ms": 1900.0},
        "gaps": [],
        "stale_after_s": 60,
    }
    args.update(overrides)
    return assemble(**args)


def test_empty_database_says_so() -> None:
    payload = build(newest=None)
    assert payload["status"] == "empty"
    assert payload["langs"] == {}


def test_all_is_the_sum_of_languages() -> None:
    langs = build()["langs"]
    assert langs["all"]["edits_5m"] == 110
    assert langs["all"]["pages_5m"] == 89
    assert langs["en"]["bot_share_5m"] == 0.2
    assert langs["de"]["edits_5m"] == 0
    assert langs["de"]["bot_share_5m"] is None  # no edits: unknown, not 0%


def test_top_articles_merge_across_languages() -> None:
    top = build()["langs"]["all"]["top_articles"]
    assert [t["title"] for t in top] == ["Portugal", "Lisboa", "Lisbon"]
    assert top[1]["url"] == "https://pt.wikipedia.org/wiki/Lisboa"


def test_per_minute_has_sixty_buckets_ending_now_and_marks_unknown_as_null() -> None:
    series = build()["langs"]["all"]["per_minute"]
    assert len(series) == MINUTES
    assert series[-1] == {"t": NEWEST_MINUTE, "edits": 30, "partial": True}
    assert series[-2] == {"t": NEWEST_MINUTE - 60, "edits": 44, "partial": False}
    assert series[0]["edits"] is None  # no rows at all: we don't know, so no fake zero
    pt = build()["langs"]["pt"]["per_minute"]
    assert pt[-1]["edits"] == 0  # minute is known, Portuguese just had none


def test_recorded_gaps_blank_out_their_minutes() -> None:
    gap = {"from_ms": (NEWEST_MINUTE - 90) * 1000, "to_ms": (NEWEST_MINUTE - 30) * 1000}
    series = build(gaps=[gap])["langs"]["all"]["per_minute"]
    assert series[-2]["edits"] is None


def test_status_flips_to_stale() -> None:
    assert build()["status"] == "live"
    stale = build(now=NEWEST + 61)
    assert stale["status"] == "stale"
    assert stale["last_event_age_s"] == 61.0


def test_the_stale_threshold_travels_with_the_data() -> None:
    # The widget reads it from here, so API and widget can't disagree about "Paused".
    assert build(stale_after_s=30)["stale_after_s"] == 30
    assert build(newest=None, stale_after_s=30)["stale_after_s"] == 30


def test_ingest_lag_is_reported_in_ms() -> None:
    assert build()["ingest_lag_ms"] == {"p50": 812, "p95": 1900}


def test_article_urls_are_encoded() -> None:
    assert article_url("en", "Python (programming language)") == (
        "https://en.wikipedia.org/wiki/Python_%28programming_language%29"
    )
    assert article_url("de", "Fußball") == "https://de.wikipedia.org/wiki/Fu%C3%9Fball"
