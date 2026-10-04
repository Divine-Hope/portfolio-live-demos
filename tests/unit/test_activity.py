from typing import Any

import pytest

from livedemos.api.activity import ActivityService, BadRequest, parse_request
from livedemos.clickhouse import QueryResult, QueryStats

LANGS = ["en", "pt", "de"]


def test_all_expands_to_every_language() -> None:
    req = parse_request(None, None, allowed=LANGS)
    assert req.langs == ("en", "pt", "de")
    assert req.window == "5m"


def test_languages_come_back_in_a_stable_order() -> None:
    assert parse_request("de, en", "1h", allowed=LANGS).langs == ("en", "de")


@pytest.mark.parametrize(
    ("lang", "window"),
    [("fr", "5m"), ("en,xx", "5m"), (",", "5m"), ("en", "7d"), ("en", "1; DROP TABLE x")],
)
def test_rejects_anything_not_allowlisted(lang: str, window: str) -> None:
    with pytest.raises(BadRequest):
        parse_request(lang, window, allowed=LANGS)


class StubClickHouse:
    def __init__(self) -> None:
        self.calls = 0

    async def query(self, sql: str, **_: Any) -> QueryResult:
        self.calls += 1
        if "count() AS n" in sql:
            return QueryResult([{"newest_ms": 1_759_600_000_000, "n": 10}], QueryStats(0.1, 1, 8))
        return QueryResult(
            [{"lang": "en", "edits": 10, "pages": 8, "bot_edits": 5}], QueryStats(1.5, 1200, 9000)
        )


async def test_second_request_is_served_from_cache() -> None:
    ch = StubClickHouse()
    service = ActivityService(ch, ttl_s=10)  # type: ignore[arg-type]
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
    assert ch.calls == 2  # NEWEST + WINDOW_TOTALS once; the hit cost nothing
