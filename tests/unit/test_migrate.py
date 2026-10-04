import pytest

from livedemos.migrate import statements


def test_renders_every_statement_for_the_database() -> None:
    sql = statements("demos_test")
    assert len(sql) == 5
    assert sql[0] == "CREATE DATABASE IF NOT EXISTS demos_test"
    assert all("{database}" not in s for s in sql)
    assert all("IF NOT EXISTS" in s for s in sql)


def test_refuses_unsafe_database_names() -> None:
    with pytest.raises(ValueError, match="invalid database name"):
        statements("demos; DROP TABLE x")
