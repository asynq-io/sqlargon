import sqlite3

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql, sqlite

from sqlargon.dialects.mysql import LOCK_TIMEOUT_SECONDS, MysqlQueryBuilder
from sqlargon.dialects.postgres import INT64_SIZE, PostgresqlQueryBuilder, _key_to_int
from sqlargon.dialects.sqlite import SQLiteQueryBuilder
from sqlargon.query_builder import (
    Option,
    QueryBuilder,
    UnsupportedOption,
    get_query_builder,
)
from sqlargon.typing import OnConflict

# Table defined at module level to avoid re-registration with --count=3
metadata = sa.MetaData()

item = sa.Table(
    "dialect_item",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.Unicode(50)),
    sa.Column("note", sa.Unicode(50)),
)

VALUES = {"id": 1, "name": "test", "note": "a note"}


@pytest.fixture
def sqlite_qb():
    return get_query_builder("sqlite")


@pytest.fixture
def postgresql_qb():
    return get_query_builder("postgresql")


@pytest.fixture
def mysql_qb():
    return get_query_builder("mysql")


def compiled(query, dialect):
    return str(query.compile(dialect=dialect))


# --- builder resolution ---


@pytest.mark.parametrize(
    ("dialect", "expected_type"),
    [
        ("postgresql", PostgresqlQueryBuilder),
        ("sqlite", SQLiteQueryBuilder),
        ("mysql", MysqlQueryBuilder),
        ("oracle", QueryBuilder),
        ("", QueryBuilder),
    ],
)
def test_get_query_builder_resolves_dialect(dialect, expected_type):
    assert type(get_query_builder(dialect)) is expected_type


def test_get_query_builder_is_cached():
    assert get_query_builder("postgresql") is get_query_builder("postgresql")


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        ("postgresql", Option.RETURNING | Option.CONFLICTS | Option.LOCKS),
        ("mysql", Option.RETURNING | Option.CONFLICTS | Option.LOCKS),
        ("oracle", Option.NONE),
    ],
)
def test_supported_options_matrix(dialect, expected):
    qb = get_query_builder(dialect)
    assert qb.supported_options == expected
    for option in (Option.RETURNING, Option.CONFLICTS, Option.LOCKS):
        assert qb.supports(option) is (option in expected)


def test_sqlite_supported_options_depend_on_sqlite_version(sqlite_qb):
    assert sqlite_qb.supports(Option.CONFLICTS)
    assert not sqlite_qb.supports(Option.LOCKS)
    assert sqlite_qb.supports(Option.RETURNING) is (sqlite3.sqlite_version > "3.35")


# --- mysql ---


def test_mysql_insert_ignore(mysql_qb):
    on_conflict = OnConflict(do="ignore", options={"index_elements": {"id"}})
    query = mysql_qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, mysql.dialect()) == (
        "INSERT IGNORE INTO dialect_item (id, name, note) VALUES (%s, %s, %s)"
    )


def test_mysql_insert_on_duplicate_key_update(mysql_qb):
    on_conflict = OnConflict(do="update", options={"set_": {"name"}})
    query = mysql_qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, mysql.dialect()) == (
        "INSERT INTO dialect_item (id, name, note) VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE name = VALUES(name)"
    )


def test_mysql_insert_on_duplicate_key_update_honours_exclude_set(mysql_qb):
    on_conflict = OnConflict(
        do="update", options={"set_": {"name", "note"}, "exclude_set": {"note"}}
    )
    query = mysql_qb.insert(item, VALUES, on_conflict=on_conflict)
    sql = compiled(query, mysql.dialect())
    assert sql.endswith("ON DUPLICATE KEY UPDATE name = VALUES(name)")
    assert "VALUES(note)" not in sql


def test_mysql_insert_without_on_conflict_is_plain_insert(mysql_qb):
    query = mysql_qb.insert(item, VALUES)
    assert compiled(query, mysql.dialect()) == (
        "INSERT INTO dialect_item (id, name, note) VALUES (%s, %s, %s)"
    )


def test_mysql_insert_without_values(mysql_qb):
    query = mysql_qb.insert(item)
    assert compiled(query, mysql.dialect()) == (
        "INSERT INTO dialect_item (id, name, note) VALUES (%s, %s, %s)"
    )


def test_mysql_upsert_multiple_values(mysql_qb):
    on_conflict = OnConflict(do="update", options={"set_": {"name"}})
    query = mysql_qb.insert(
        item,
        [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        on_conflict=on_conflict,
    )
    assert compiled(query, mysql.dialect()) == (
        "INSERT INTO dialect_item (id, name) VALUES (%s, %s), (%s, %s) "
        "ON DUPLICATE KEY UPDATE name = VALUES(name)"
    )


def test_mysql_lock(mysql_qb):
    lock = mysql_qb.lock("my-key")
    assert str(lock) == "SELECT GET_LOCK(:key, :timeout)"
    assert lock.compile().params == {
        "key": "my-key",
        "timeout": LOCK_TIMEOUT_SECONDS,
    }


def test_mysql_unlock(mysql_qb):
    unlock = mysql_qb.unlock("my-key")
    assert str(unlock) == "SELECT RELEASE_LOCK(:key)"
    assert unlock.compile().params == {"key": "my-key"}


def test_mysql_get_lock_pair(mysql_qb):
    lock, unlock = mysql_qb.get_lock_pair("my-key")
    assert str(lock) == "SELECT GET_LOCK(:key, :timeout)"
    assert str(unlock) == "SELECT RELEASE_LOCK(:key)"
    assert unlock.compile().params == {"key": "my-key"}


def test_mysql_lock_timeout_is_positive():
    # MariaDB answers NULL to a negative timeout instead of taking the lock
    assert LOCK_TIMEOUT_SECONDS > 0


# --- postgresql ---


def test_postgresql_insert_do_nothing_with_index_elements(postgresql_qb):
    on_conflict = OnConflict(do="ignore", options={"index_elements": {"id"}})
    query = postgresql_qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, postgresql.dialect()).endswith("ON CONFLICT (id) DO NOTHING")


def test_postgresql_insert_do_nothing_with_constraint(postgresql_qb):
    on_conflict = OnConflict(do="ignore", options={"constraint": "dialect_item_pkey"})
    query = postgresql_qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, postgresql.dialect()).endswith(
        "ON CONFLICT ON CONSTRAINT dialect_item_pkey DO NOTHING"
    )


def test_postgresql_insert_do_nothing_with_index_where(postgresql_qb):
    on_conflict = OnConflict(
        do="ignore",
        options={"index_elements": {"id"}, "index_where": item.c.note.is_(None)},
    )
    query = postgresql_qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, postgresql.dialect()).endswith(
        "ON CONFLICT (id) WHERE note IS NULL DO NOTHING"
    )


def test_postgresql_insert_do_update_honours_exclude_set(postgresql_qb):
    on_conflict = OnConflict(
        do="update",
        options={
            "index_elements": {"id"},
            "set_": {"name", "note"},
            "exclude_set": {"note"},
        },
    )
    query = postgresql_qb.insert(item, VALUES, on_conflict=on_conflict)
    sql = compiled(query, postgresql.dialect())
    assert sql.endswith("ON CONFLICT (id) DO UPDATE SET name = excluded.name")
    assert "excluded.note" not in sql


def test_postgresql_insert_do_update_with_where(postgresql_qb):
    on_conflict = OnConflict(
        do="update",
        options={
            "index_elements": {"id"},
            "set_": {"name"},
            "where": item.c.note.is_(None),
        },
    )
    query = postgresql_qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, postgresql.dialect()).endswith(
        "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
        "WHERE dialect_item.note IS NULL"
    )


def test_postgresql_lock_and_unlock_share_the_key(postgresql_qb):
    lock = postgresql_qb.lock("my-key")
    unlock = postgresql_qb.unlock("my-key")
    assert str(lock) == "SELECT pg_advisory_lock(:key)"
    assert str(unlock) == "SELECT pg_advisory_unlock(:key)"
    assert lock.compile().params == unlock.compile().params
    assert lock.compile().params == {"key": _key_to_int("my-key")}


def test_postgresql_lock_binds_a_distinct_key_per_name(postgresql_qb):
    assert (
        postgresql_qb.lock("a").compile().params
        != postgresql_qb.lock("b").compile().params
    )


@pytest.mark.parametrize("key", ["", "a", "some-lock-key", "ключ", "x" * 500])
def test_key_to_int_fits_in_int64(key):
    int_key = _key_to_int(key)
    assert 0 <= int_key < INT64_SIZE
    assert int_key == _key_to_int(key)


# --- sqlite ---


def test_sqlite_insert_do_nothing_with_index_where(sqlite_qb):
    on_conflict = OnConflict(
        do="ignore",
        options={"index_elements": {"id"}, "index_where": item.c.note.is_(None)},
    )
    query = sqlite_qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, sqlite.dialect()).endswith(
        "ON CONFLICT (id) WHERE note IS NULL DO NOTHING"
    )


def test_sqlite_insert_do_update_honours_exclude_set(sqlite_qb):
    on_conflict = OnConflict(
        do="update",
        options={
            "index_elements": {"id"},
            "set_": {"name", "note"},
            "exclude_set": {"note"},
        },
    )
    query = sqlite_qb.insert(item, VALUES, on_conflict=on_conflict)
    sql = compiled(query, sqlite.dialect())
    assert sql.endswith("ON CONFLICT (id) DO UPDATE SET name = excluded.name")
    assert "excluded.note" not in sql


def test_sqlite_insert_do_update_with_where(sqlite_qb):
    on_conflict = OnConflict(
        do="update",
        options={
            "index_elements": {"id"},
            "set_": {"name"},
            "where": item.c.note.is_(None),
        },
    )
    query = sqlite_qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, sqlite.dialect()).endswith(
        "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
        "WHERE dialect_item.note IS NULL"
    )


def test_sqlite_insert_without_on_conflict_is_plain_insert(sqlite_qb):
    query = sqlite_qb.insert(item, VALUES)
    assert compiled(query, sqlite.dialect()) == (
        "INSERT INTO dialect_item (id, name, note) VALUES (?, ?, ?)"
    )


# --- expression valued set_ ---


@pytest.mark.parametrize(
    ("dialect", "compiler"),
    [("postgresql", postgresql.dialect()), ("sqlite", sqlite.dialect())],
)
def test_insert_do_update_with_an_expression(dialect, compiler):
    qb = get_query_builder(dialect)
    excluded = qb.excluded(item)
    on_conflict = OnConflict(
        do="update",
        options={
            "index_elements": {"id"},
            "set_": {"name": excluded.name, "note": item.c.note + excluded.note},
        },
    )
    query = qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, compiler).endswith(
        "ON CONFLICT (id) DO UPDATE SET name = excluded.name, "
        "note = (dialect_item.note || excluded.note)"
    )


@pytest.mark.parametrize(
    ("dialect", "compiler"),
    [
        ("postgresql", postgresql.dialect()),
        ("sqlite", sqlite.dialect()),
        ("mysql", mysql.dialect()),
    ],
)
def test_insert_do_update_expression_honours_exclude_set(dialect, compiler):
    qb = get_query_builder(dialect)
    on_conflict = OnConflict(
        do="update",
        options={
            "index_elements": {"id"},
            "set_": {"name": item.c.name, "note": item.c.note},
            "exclude_set": {"note"},
        },
    )
    query = qb.insert(item, VALUES, on_conflict=on_conflict)
    assert compiled(query, compiler).endswith("name = dialect_item.name")


@pytest.mark.parametrize("qb", [QueryBuilder(), MysqlQueryBuilder()])
def test_excluded_row_is_unsupported(qb):
    """MySQL renders it as ``VALUES(col)``, which cannot be built up front."""
    with pytest.raises(UnsupportedOption, match="dialect_item"):
        qb.excluded(item)


# --- unreachable on_conflict.do ---


@pytest.mark.parametrize("dialect", ["postgresql", "sqlite", "mysql"])
@pytest.mark.parametrize("do", ["bogus", "IGNORE", None])
def test_insert_with_unknown_on_conflict_do_is_unreachable(dialect, do):
    qb = get_query_builder(dialect)
    on_conflict = OnConflict(do=do, options={"index_elements": {"id"}})
    with pytest.raises(AssertionError, match="Expected code to be unreachable"):
        qb.insert(item, VALUES, on_conflict=on_conflict)
