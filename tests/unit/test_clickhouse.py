import json

import httpx
import pytest

from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import ClickHouseSettings


def client(handler: httpx.MockTransport) -> ClickHouse:
    http = httpx.AsyncClient(base_url="http://ch", transport=handler)
    return ClickHouse(ClickHouseSettings(url="http://ch"), client=http)


def respond(status: int, body: str) -> httpx.MockTransport:
    return httpx.MockTransport(lambda _: httpx.Response(status, text=body))


async def test_rows_and_statistics() -> None:
    body = {
        "data": [{"n": 1}],
        "statistics": {"elapsed": 0.0015, "rows_read": 10, "bytes_read": 80},
    }
    result = await client(respond(200, json.dumps(body))).query("SELECT 1 AS n")
    assert result.rows == [{"n": 1}]
    assert (result.stats.elapsed_ms, result.stats.rows_read) == (1.5, 10)


@pytest.mark.parametrize(
    ("status", "body", "message"),
    [
        (500, "Code: 241. DB::Exception: Memory limit exceeded", "Code: 241"),
        # HTTP 200 isn't success: an error after rows started streaming lands in the body.
        (200, '{"data": [{"n": 1}\nCode: 159. DB::Exception: Timeout exceeded', "Code: 159"),
        (200, '{"meta": [], "data": [], "exception": "Code: 159. Timeout exceeded"}', "Code: 159"),
        (200, "not json at all", "not json"),
        (200, "[1, 2]", "unexpected response shape"),
        (200, '{"data": {"n": 1}}', "shape: data"),
        (200, '{"data": [1, 2]}', "shape: data"),
        (200, '{"data": [], "statistics": []}', "shape: statistics"),
        (200, '{"data": [], "statistics": {"elapsed": "soon"}}', "shape: statistics"),
    ],
    ids=[
        "http-error",
        "truncated",
        "exception-key",
        "not-json",
        "wrong-shape",
        "data-not-list",
        "rows-not-objects",
        "statistics-not-object",
        "statistics-not-numbers",
    ],
)
async def test_every_failure_is_a_clickhouse_error(status: int, body: str, message: str) -> None:
    with pytest.raises(ClickHouseError, match=message):
        await client(respond(status, body)).query("SELECT 1")


async def test_insert_names_the_query_and_binds_the_token() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    ch = client(httpx.MockTransport(handler))
    await ch.insert("t", [{"a": 1}], dedup_token="tok", query_id="ingest-tok")
    params = seen[0].url.params
    assert params["insert_deduplication_token"] == "tok"
    assert params["query_id"] == "ingest-tok"
    assert params["query"] == "INSERT INTO demos.t FORMAT JSONEachRow"


async def test_unreachable_is_a_clickhouse_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(ClickHouseError, match="unreachable"):
        await client(httpx.MockTransport(handler)).query("SELECT 1")
