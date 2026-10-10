"""A small async client for ClickHouse's HTTP interface.

Why not a driver? The HTTP interface already gives us everything this project needs:
JSONEachRow inserts with a deduplication token, parameterised queries, and per-query
statistics (elapsed time, rows read) in the JSON response. Keeping it to one file makes
the data path easy to read end to end.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from livedemos.config import ClickHouseSettings

# Return numbers as numbers and timestamps as ISO 8601, so the API can pass them through.
# A URL setting rather than "FORMAT JSON" appended to the SQL, which a trailing ";" or
# comment would break.
_READ_SETTINGS = {
    "default_format": "JSON",
    "output_format_json_quote_64bit_integers": "0",
    "date_time_output_format": "iso",
}
# Deduplicating in the materialized views too is the writer profile's job (users.d).
# A misspelled column fails the insert instead of being dropped.
_WRITE_SETTINGS = {
    "date_time_input_format": "best_effort",
    "input_format_skip_unknown_fields": "0",
}


class ClickHouseError(RuntimeError):
    """ClickHouse answered with an error, or couldn't be reached."""


@dataclass(frozen=True, slots=True)
class QueryStats:
    elapsed_ms: float
    rows_read: int
    bytes_read: int


@dataclass(frozen=True, slots=True)
class QueryResult:
    rows: list[dict[str, Any]]
    stats: QueryStats


class Queryable(Protocol):
    """What the read side needs from a database. Lets services take a stub in tests."""

    async def query(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> QueryResult: ...


class Database(Queryable, Protocol):
    """What ingest needs: reads, plus idempotent batch inserts."""

    async def insert(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        dedup_token: str | None = None,
        query_id: str | None = None,
    ) -> None: ...


class ClickHouse:
    def __init__(self, settings: ClickHouseSettings, client: httpx.AsyncClient | None = None):
        self._settings = settings
        self._client = client or httpx.AsyncClient(
            base_url=settings.url,
            timeout=settings.timeout_s,
            headers={
                "X-ClickHouse-User": settings.user,
                "X-ClickHouse-Key": settings.password,
            },
        )

    @property
    def database(self) -> str:
        return self._settings.database

    async def aclose(self) -> None:
        await self._client.aclose()

    async def ping(self) -> bool:
        try:
            response = await self._client.get("/ping")
        except httpx.HTTPError:
            return False
        return response.status_code == 200

    async def execute(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> None:
        """Run a statement that returns no rows (DDL, INSERT ... SELECT, ALTER ... DELETE)."""
        await self._post(sql, params={**(settings or {}), **_bind(params)})

    async def query(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> QueryResult:
        """Run a SELECT and return rows plus ClickHouse's own statistics.

        Use ``{name:Type}`` placeholders in ``sql`` and pass values in ``params``;
        ClickHouse binds them server-side, so nothing is string-formatted into SQL.
        """
        query_params = {**_READ_SETTINGS, **(settings or {}), **_bind(params)}
        response = await self._post(sql, params=query_params)
        # HTTP 200 doesn't mean the query succeeded: once rows start streaming, an error
        # can only be written into the body. Either way it must surface as ClickHouseError.
        try:
            body = response.json()
        except ValueError as exc:
            raise ClickHouseError(_error_text(response.text) or "invalid JSON response") from exc
        if not isinstance(body, dict):
            raise ClickHouseError("unexpected response shape")
        if body.get("exception"):
            raise ClickHouseError(str(body["exception"])[:500])
        rows, stats = body.get("data", []), body.get("statistics", {})
        if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
            raise ClickHouseError("unexpected response shape: data")
        if not isinstance(stats, dict):
            raise ClickHouseError("unexpected response shape: statistics")
        try:
            query_stats = QueryStats(
                elapsed_ms=round(float(stats.get("elapsed", 0.0)) * 1000, 3),
                rows_read=int(stats.get("rows_read", 0)),
                bytes_read=int(stats.get("bytes_read", 0)),
            )
        except (TypeError, ValueError) as exc:
            raise ClickHouseError("unexpected response shape: statistics") from exc
        return QueryResult(rows=rows, stats=query_stats)

    async def insert(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        dedup_token: str | None = None,
        query_id: str | None = None,
    ) -> None:
        """Insert rows in one request.

        Atomic only if the rows land in one block of one partition: the caller keeps a
        batch to one partition, and a batch is far below max_insert_block_size (~1M rows).

        ``dedup_token`` makes a retried insert of the same batch a no-op, which keeps
        a timeout-then-retry from writing the batch twice. ``query_id`` names the insert
        in system.processes; ClickHouse also refuses a second query with an id that is
        still running.
        """
        if not rows:
            return
        params = dict(_WRITE_SETTINGS)
        if dedup_token:
            params["insert_deduplication_token"] = dedup_token
        if query_id:
            params["query_id"] = query_id
        params["query"] = f"INSERT INTO {self.database}.{table} FORMAT JSONEachRow"
        body = "\n".join(json.dumps(row, separators=(",", ":"), default=str) for row in rows)
        await self._send(params=params, content=body.encode())

    async def _post(self, sql: str, *, params: dict[str, str]) -> httpx.Response:
        return await self._send(params=params, content=sql.encode())

    async def _send(self, *, params: dict[str, str], content: bytes) -> httpx.Response:
        params = {"database": self.database, **params}
        try:
            response = await self._client.post("/", params=params, content=content)
        except httpx.HTTPError as exc:
            raise ClickHouseError(f"ClickHouse unreachable: {exc!r}") from exc
        if response.status_code != 200:
            raise ClickHouseError(_error_text(response.text) or f"HTTP {response.status_code}")
        return response


def _error_text(text: str) -> str:
    text = text.strip()
    start = text.find("Code: ")  # the exception, even after partial output
    return (text[start:] if start >= 0 else text)[:500]


def _bind(params: Mapping[str, Any] | None) -> dict[str, str]:
    return {f"param_{key}": _param_value(value) for key, value in (params or {}).items()}


def _param_value(value: Any) -> str:
    # ClickHouse reads a parameter in its escaped text format: a backslash, tab or newline
    # in a plain value is an escape, and an array is a literal with quoted strings.
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_array_element(v) for v in value) + "]"
    return _escape(str(value))


def _array_element(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    return quote(str(value))


def _escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


def quote(text: str) -> str:
    """A ClickHouse string literal."""
    return "'" + _escape(text).replace("'", "\\'") + "'"
