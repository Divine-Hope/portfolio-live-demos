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
from typing import Any

import httpx

from livedemos.config import ClickHouseSettings

# Return numbers as numbers and timestamps as ISO 8601, so the API can pass them through.
_READ_SETTINGS = {
    "output_format_json_quote_64bit_integers": "0",
    "date_time_output_format": "iso",
}
_WRITE_SETTINGS = {
    "date_time_input_format": "best_effort",
    "input_format_skip_unknown_fields": "1",
    # A deduplicated retry must not double-count in materialized views either.
    "deduplicate_blocks_in_dependent_materialized_views": "1",
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

    async def execute(self, sql: str, *, settings: Mapping[str, str] | None = None) -> None:
        """Run a statement that returns no rows (DDL, INSERT ... SELECT)."""
        await self._post(sql, params=dict(settings or {}))

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
        query_params: dict[str, str] = {**_READ_SETTINGS, **(settings or {})}
        for key, value in (params or {}).items():
            query_params[f"param_{key}"] = _param_value(value)
        response = await self._post(f"{sql}\nFORMAT JSON", params=query_params)
        body = response.json()
        stats = body.get("statistics", {})
        return QueryResult(
            rows=body.get("data", []),
            stats=QueryStats(
                elapsed_ms=round(float(stats.get("elapsed", 0.0)) * 1000, 3),
                rows_read=int(stats.get("rows_read", 0)),
                bytes_read=int(stats.get("bytes_read", 0)),
            ),
        )

    async def insert(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        dedup_token: str | None = None,
    ) -> None:
        """Insert rows as one block. One block is atomic in a MergeTree table.

        ``dedup_token`` makes a retried insert of the same batch a no-op, which keeps
        a timeout-then-retry from writing the batch twice.
        """
        if not rows:
            return
        params = dict(_WRITE_SETTINGS)
        if dedup_token:
            params["insert_deduplication_token"] = dedup_token
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
            text = response.text.strip()
            start = text.find("Code: ")  # the exception, even after partial output
            raise ClickHouseError((text[start:] if start >= 0 else text)[:500])
        return response


def _param_value(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(json.dumps(v) for v in value).replace('"', "'") + "]"
    return str(value)
