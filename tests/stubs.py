"""A stand-in for ClickHouse in unit tests: answers queries, records inserts."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

from livedemos.db.clickhouse import ClickHouseError, QueryResult, QueryStats

NO_STATS = QueryStats(0.0, 0, 0)


class StubClickHouse:
    """Implements the client's protocols. Override `answer` for what a query returns."""

    def __init__(
        self, *, delay_s: float = 0.0, fail_queries: bool = False, fail_inserts: int = 0
    ) -> None:
        self.delay_s = delay_s
        self.fail_queries = fail_queries
        self.fail_inserts = fail_inserts
        self.queries: list[str] = []
        # Every attempt, failed or not: (table, rows, dedup token, query id).
        self.inserts: list[tuple[str, list[Mapping[str, Any]], str | None, str | None]] = []
        self.committed: list[Mapping[str, Any]] = []  # rows of the attempts that succeeded

    def answer(self, sql: str, params: Mapping[str, Any]) -> QueryResult:
        return QueryResult([], NO_STATS)

    async def query(
        self,
        sql: str,
        *,
        params: Mapping[str, Any] | None = None,
        settings: Mapping[str, str] | None = None,
    ) -> QueryResult:
        self.queries.append(sql)
        await asyncio.sleep(self.delay_s)
        if self.fail_queries:
            raise ClickHouseError("down")
        return self.answer(sql, params or {})

    async def execute(self, sql: str, *, params: Any = None, settings: Any = None) -> None:
        await self.query(sql, params=params)

    async def insert(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        dedup_token: str | None = None,
        query_id: str | None = None,
    ) -> None:
        self.inserts.append((table, list(rows), dedup_token, query_id))
        if self.fail_inserts:
            self.fail_inserts -= 1
            raise ClickHouseError("read timeout (did it commit? nobody knows)")
        self.committed.extend(rows)
