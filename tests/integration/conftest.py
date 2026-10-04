"""Integration tests need a real ClickHouse. `make test-integration` starts one."""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator

import pytest

from livedemos.clickhouse import ClickHouse
from livedemos.config import ClickHouseSettings
from livedemos.migrate import migrate

TEST_DB = "demos_test"


def clickhouse_test_settings() -> ClickHouseSettings:
    return ClickHouseSettings(
        url=os.environ.get("CLICKHOUSE_URL", "http://localhost:8123"),
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=TEST_DB,
    )


@pytest.fixture
async def ch() -> AsyncIterator[ClickHouse]:
    """A ClickHouse client on an empty, freshly migrated test database."""
    client = ClickHouse(clickhouse_test_settings())
    if not await client.ping():
        await client.aclose()
        pytest.fail("ClickHouse isn't reachable. Run `make test-integration`.")
    await client.execute(
        f"DROP DATABASE IF EXISTS {TEST_DB} SYNC", settings={"database": "default"}
    )
    await migrate(client)
    yield client
    await client.aclose()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port
