"""Integration tests need a real ClickHouse. `make test-integration` starts one."""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import SecretStr

from livedemos.config import ClickHouseSettings
from livedemos.db.clickhouse import ClickHouse
from livedemos.db.migrate import migrate
from livedemos.ingest.events import Edit, to_row

TEST_DB = "demos_test"


def clickhouse_test_settings() -> ClickHouseSettings:
    return ClickHouseSettings(
        url=os.environ.get("CLICKHOUSE_URL", "http://localhost:8123"),
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=SecretStr(os.environ.get("CLICKHOUSE_PASSWORD", "")),
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


def rows(
    n: int,
    *,
    lang: str = "en",
    start: datetime | None = None,
    bot_every: int = 0,
    first_seq: int = 1,
) -> list[dict[str, object]]:
    start = start or datetime.now(UTC) - timedelta(seconds=n)
    out = []
    for i in range(n):
        edit = Edit(
            event_id=str(uuid4()),
            event_time=start + timedelta(seconds=i),
            wiki=f"{lang}wiki",
            lang=lang,
            type="edit",
            namespace=0,
            title=f"Article {i % 3}",
            is_bot=bool(bot_every) and i % bot_every == 0,
        )
        out.append(
            to_row(
                edit,
                sse_id=f'[{{"timestamp":{i}}}]',
                ingest_seq=first_seq + i,
                ingested_at=datetime.now(UTC),
            )
        )
    return out


def user_settings(user: str, password_env: str) -> ClickHouseSettings:
    password = os.environ.get(password_env)
    if not password:
        pytest.skip(f"{password_env} not set")
    return clickhouse_test_settings().model_copy(
        update={"user": user, "password": SecretStr(password), "database": "demos"}
    )


@pytest.fixture
async def demos_schema() -> AsyncIterator[None]:
    """The users' grants are on `demos`. Create it if this ClickHouse has never had it (CI);
    a local stack's `demos` already exists and is left alone."""
    admin = ClickHouse(clickhouse_test_settings().model_copy(update={"database": "demos"}))
    try:
        # Asked from `default`: a connection to `demos` fails if `demos` doesn't exist yet.
        exists = await admin.query(
            "SELECT count() AS n FROM system.databases WHERE name = 'demos'",
            settings={"database": "default"},
        )
        if not int(exists.rows[0]["n"]):
            await migrate(admin)
        yield
    finally:
        await admin.aclose()
