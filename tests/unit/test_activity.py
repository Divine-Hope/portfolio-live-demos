import asyncio
from collections.abc import Mapping
from typing import Any

import pytest

from livedemos.api import queries
from livedemos.api.activity import ActivityService, BadRequest, Unavailable, parse_request
from livedemos.config import ApiSettings
from livedemos.db.clickhouse import ClickHouseError, QueryResult, QueryStats
from tests import stubs

LANGS = ["en", "pt", "de"]


def settings(**overrides: Any) -> ApiSettings:
    return ApiSettings(**{"activity_cache_ttl_s": 10, **overrides})


def test_all_expands_to_every_language() -> None:
    req = parse_request(None, None, allowed=LANGS)
    assert req.langs == ("en", "pt", "de")
    assert req.window == "5m"


def test_languages_come_back_in_a_stable_order() -> None:
    assert parse_request("de, en", "1h", allowed=LANGS).langs == ("en", "de")


@pytest.mark.parametrize(
    ("lang", "window"),
    [("fr", "5m"), ("en,xx", "5m"), (",", "5m"), ("en", "30d"), ("en", "1; DROP TABLE x")],
)
def test_rejects_anything_not_allowlisted(lang: str, window: str) -> None:
    with pytest.raises(BadRequest):
        parse_request(lang, window, allowed=LANGS)


class StubClickHouse(stubs.StubClickHouse):
    def answer(self, sql: str, params: Mapping[str, Any]) -> QueryResult:
        if sql is queries.NEWEST:
            return QueryResult([{"newest_ms": 1_759_600_000_000, "n": 10}], QueryStats(0.1, 1, 8))
        return QueryResult(
            [{"lang": "en", "edits": 10, "pages": 8, "bot_edits": 5}], QueryStats(1.5, 1200, 9000)
        )


async def test_second_request_is_served_from_cache() -> None:
    ch = StubClickHouse()
    service = ActivityService(ch, settings())
    req = parse_request("en", "5m", allowed=LANGS)
    first = await service.get(req)
    second = await service.get(req)
    assert first["query"] == {
        "elapsed_ms": 1.5,
        "rows_read": 1200,
        "bytes_read": 9000,
        "cache": "miss",
    }
    assert second["query"]["cache"] == "hit"
    assert first["bot_share"] == 0.5
    assert len(ch.queries) == 2  # NEWEST + WINDOW_TOTALS once; the hit cost nothing


async def test_concurrent_misses_share_one_query() -> None:
    ch = StubClickHouse(delay_s=0.05)
    service = ActivityService(ch, settings())
    req = parse_request("en", "1h", allowed=LANGS)
    results = await asyncio.gather(*(service.get(req) for _ in range(20)))
    assert len(ch.queries) == 2  # NEWEST + WINDOW_TOTALS, once, for all twenty visitors
    assert all(r["edits"] == 10 for r in results)


async def test_no_request_waits_longer_than_the_deadline() -> None:
    service = ActivityService(StubClickHouse(delay_s=1.0), settings(activity_wait_s=0.05))
    with pytest.raises(Unavailable, match="busy"):
        await service.get(parse_request("en", "5m", allowed=LANGS))
    await service.aclose()


async def test_a_failure_is_remembered_instead_of_retried_by_everyone() -> None:
    ch = StubClickHouse(fail_queries=True)
    service = ActivityService(ch, settings(activity_error_cooldown_s=60))
    req = parse_request("en", "5m", allowed=LANGS)
    with pytest.raises(Unavailable, match="query failed"):
        await service.get(req)
    calls = len(ch.queries)
    for _ in range(5):
        with pytest.raises(Unavailable, match="recently"):
            await service.get(req)
    assert len(ch.queries) == calls  # the cooldown cost ClickHouse nothing


async def test_queries_across_keys_are_capped() -> None:
    running = 0
    peak = 0

    class Counting(StubClickHouse):
        async def query(self, sql: str, **kwargs: Any) -> QueryResult:
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            try:
                return await super().query(sql, **kwargs)
            finally:
                running -= 1

    service = ActivityService(
        Counting(delay_s=0.02),
        settings(activity_max_concurrency=2, activity_max_pending=15),
    )
    keys = [
        parse_request(lang, w, allowed=LANGS)
        for lang in LANGS
        for w in ("5m", "1h", "24h", "3d", "7d")
    ]
    await asyncio.gather(*(service.get(k) for k in keys))
    assert peak <= 2


async def test_misses_beyond_the_admission_cap_are_shed_at_once() -> None:
    service = ActivityService(
        StubClickHouse(delay_s=0.2),
        settings(activity_max_concurrency=1, activity_max_pending=2),
    )
    keys = [parse_request(lang, "5m", allowed=LANGS) for lang in LANGS]
    results = await asyncio.gather(*(service.get(k) for k in keys), return_exceptions=True)
    shed = [r for r in results if isinstance(r, Unavailable)]
    assert len(shed) == 1  # the third distinct key, refused without queueing
    await service.aclose()


async def test_a_malformed_response_counts_as_a_failure() -> None:
    class Garbled(StubClickHouse):
        async def query(self, sql: str, **kwargs: Any) -> QueryResult:
            raise ClickHouseError("unexpected response shape: statistics")

    service = ActivityService(Garbled(), settings(activity_error_cooldown_s=60))
    req = parse_request("en", "5m", allowed=LANGS)
    with pytest.raises(Unavailable, match="query failed"):
        await service.get(req)
    with pytest.raises(Unavailable, match="recently"):
        await service.get(req)


async def test_a_long_window_older_than_the_kept_page_sets_has_no_page_count() -> None:
    """The stub's newest event is in 2025: far past the 14 days of page sets."""
    service = ActivityService(StubClickHouse(), settings())
    week = await service.get(parse_request("en", "7d", allowed=LANGS))
    assert week["pages_edited"] is None
    assert week["edits"] == 10  # edits come from the 90-day rollup, still there
    day = await service.get(parse_request("en", "24h", allowed=LANGS))
    assert day["pages_edited"] == 8
