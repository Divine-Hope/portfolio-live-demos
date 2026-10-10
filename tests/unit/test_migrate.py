import pytest

from livedemos.db.migrate import load


def test_migrations_are_numbered_in_order_and_rendered() -> None:
    migrations = load("demos_test")
    assert [m.version for m in migrations] == list(range(1, len(migrations) + 1))
    assert migrations[0].statements[0] == "CREATE DATABASE IF NOT EXISTS demos_test"
    assert all("{database}" not in sql for m in migrations for sql in m.statements)


def test_checksums_ignore_comments_but_not_statements() -> None:
    a, b = load("demos_test"), load("other_db")
    assert a[0].checksum != b[0].checksum  # rendered for a different database
    assert [m.checksum for m in a] == [m.checksum for m in load("demos_test")]


def test_refuses_unsafe_database_names() -> None:
    with pytest.raises(ValueError, match="invalid database name"):
        load("demos; DROP TABLE x")
