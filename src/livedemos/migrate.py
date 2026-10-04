"""Apply schema.sql. Safe to run on every start: every statement is IF NOT EXISTS."""

from __future__ import annotations

import asyncio
import logging
import re
from importlib.resources import files

from livedemos.clickhouse import ClickHouse
from livedemos.config import clickhouse_settings
from livedemos.logs import setup_logging

log = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_END_OF_STATEMENT = re.compile(r";[ \t]*$", re.MULTILINE)


def statements(database: str) -> list[str]:
    if not _IDENTIFIER.match(database):
        raise ValueError(f"invalid database name: {database!r}")
    text = files("livedemos").joinpath("schema.sql").read_text()
    without_comments = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("--")
    )
    rendered = without_comments.replace("{database}", database)
    # Statements end with a semicolon at the end of a line, so a ';' inside a string is safe.
    return [stmt.strip() for stmt in _END_OF_STATEMENT.split(rendered) if stmt.strip()]


async def migrate(ch: ClickHouse) -> None:
    # CREATE DATABASE must not run inside the database it creates.
    for sql in statements(ch.database):
        if sql.upper().startswith("CREATE DATABASE"):
            await ch.execute(sql, settings={"database": "default"})
        else:
            await ch.execute(sql)
    log.info("schema up to date", extra={"database": ch.database})


async def _main() -> None:
    setup_logging()
    ch = ClickHouse(clickhouse_settings())
    try:
        await migrate(ch)
    finally:
        await ch.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
