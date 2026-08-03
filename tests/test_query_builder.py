import pytest
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from sqlargon.query_builder import (
    Option,
    QueryBuilder,
    UnsupportedOption,
    get_query_builder,
)
from sqlargon.typing import OnConflict


@pytest.fixture
def qb():
    return QueryBuilder()


@pytest.fixture
def sqlite_qb():
    return get_query_builder("sqlite")


@pytest.fixture
def postgresql_qb():
    return get_query_builder("postgresql")


@pytest.fixture
def mysql_qb():
    return get_query_builder("mysql")


def test_base_query_builder_supports_nothing(qb):
    assert not qb.supports(Option.RETURNING)
    assert not qb.supports(Option.CONFLICTS)
    assert not qb.supports(Option.LOCKS)


def test_query_builder_select(qb):
    query = qb.select(sa.literal(1))
    assert query is not None


def test_query_builder_select_with_for_update(qb):
    query = qb.select(sa.literal(1), with_for_update={"nowait": True})
    assert query is not None


def test_query_builder_select_with_for_update_true(qb):
    query = qb.select(sa.literal(1), with_for_update=True)
    assert str(query.compile(dialect=postgresql.dialect())).endswith("FOR UPDATE")


def test_query_builder_select_with_for_update_options(qb):
    query = qb.select(sa.literal(1), with_for_update={"nowait": True, "read": True})
    assert str(query.compile(dialect=postgresql.dialect())).endswith("FOR SHARE NOWAIT")


@pytest.mark.parametrize("with_for_update", [None, False])
def test_query_builder_select_without_for_update(qb, with_for_update):
    query = qb.select(sa.literal(1), with_for_update=with_for_update)
    assert "FOR UPDATE" not in str(query.compile(dialect=postgresql.dialect()))


def test_query_builder_select_with_options(qb, user_model):
    query = qb.select(user_model, options=(orm.load_only(user_model.name),))
    sql = str(query)
    assert "name" in sql
    assert "last_name" not in sql


def test_query_builder_select_without_options_loads_all_columns(qb, user_model):
    assert "last_name" in str(qb.select(user_model))


def test_query_builder_insert(qb):
    t = sa.table("t", sa.column("a"))
    query = qb.insert(t, {"a": 1})
    assert query is not None


def test_query_builder_insert_returning_unsupported(qb):
    t = sa.table("t", sa.column("a"))
    with pytest.raises(UnsupportedOption):
        qb.insert(t, {"a": 1}, return_results=True)


@pytest.mark.parametrize("do", ["ignore", "update"])
def test_query_builder_insert_on_conflict_unsupported(qb, do):
    t = sa.table("t", sa.column("a"))
    on_conflict = OnConflict(do=do, options={"index_elements": {"a"}, "set_": {"a"}})
    with pytest.raises(UnsupportedOption):
        qb.insert(t, {"a": 1}, on_conflict=on_conflict)


def test_query_builder_update(qb):
    t = sa.table("t", sa.column("a"))
    query = qb.update(t, {"a": 1})
    assert query is not None


def test_query_builder_update_returning_unsupported(qb):
    t = sa.table("t", sa.column("a"))
    with pytest.raises(UnsupportedOption):
        qb.update(t, {"a": 1}, return_results=True)


def test_query_builder_delete(qb):
    t = sa.table("t")
    query = qb.delete(t)
    assert query is not None


def test_query_builder_delete_returning_unsupported(qb):
    t = sa.table("t")
    with pytest.raises(UnsupportedOption):
        qb.delete(t, return_results=True)


def test_query_builder_filter_args(qb):
    query = qb.select(sa.literal(1).label("x"))
    filtered = qb.filter(query, sa.literal(value=True))
    assert filtered is not None


def test_query_builder_filter_kwargs(qb):
    t = sa.table("t", sa.column("name"))
    query = qb.select(t)
    filtered = qb.filter(query, name="test")
    assert filtered is not None


def test_query_builder_count(qb):
    t = sa.table("t")
    query = qb.count(t)
    assert query is not None


def test_query_builder_page_without_total(qb):
    query = sa.select(sa.literal(1))
    page_query, total_query = qb.page(query, offset=0, limit=10, include_total=False)
    assert page_query is not None
    assert total_query is None


def test_query_builder_page_with_total(qb):
    query = sa.select(sa.literal(1))
    page_query, total_query = qb.page(query, offset=0, limit=10, include_total=True)
    assert page_query is not None
    assert total_query is not None


def test_query_builder_lock_not_implemented(qb):
    with pytest.raises(NotImplementedError):
        qb.lock("test")


def test_query_builder_unlock_not_implemented(qb):
    with pytest.raises(NotImplementedError):
        qb.unlock("test")


def test_query_builder_get_lock_pair_unsupported(qb):
    with pytest.raises(UnsupportedOption):
        qb.get_lock_pair("test")


@pytest.mark.parametrize("dialect", ["postgresql", "mysql"])
def test_query_builder_get_lock_pair(dialect):
    qb = get_query_builder(dialect)
    lock, unlock = qb.get_lock_pair("test")
    assert str(lock) == str(qb.lock("test"))
    assert str(unlock) == str(qb.unlock("test"))
    assert lock.compile().params == unlock.compile().params


def test_sqlite_get_lock_pair_unsupported(sqlite_qb):
    with pytest.raises(UnsupportedOption):
        sqlite_qb.get_lock_pair("test")


def test_sqlite_query_builder_options(sqlite_qb):
    assert sqlite_qb.supports(Option.RETURNING)
    assert sqlite_qb.supports(Option.CONFLICTS)
    assert not sqlite_qb.supports(Option.LOCKS)


def test_sqlite_insert_do_nothing(sqlite_qb):
    t = sa.table("t", sa.column("id"), sa.column("name"))
    on_conflict = OnConflict(do="ignore", options={"index_elements": {"id"}})
    query = sqlite_qb.insert(t, {"id": 1, "name": "test"}, on_conflict=on_conflict)
    assert query is not None


def test_sqlite_insert_do_update(sqlite_qb):
    t = sa.table("t", sa.column("id"), sa.column("name"))
    on_conflict = OnConflict(
        do="update", options={"index_elements": {"id"}, "set_": {"name"}}
    )
    query = sqlite_qb.insert(t, {"id": 1, "name": "test"}, on_conflict=on_conflict)
    assert query is not None


def test_sqlite_insert_returning(sqlite_qb):
    t = sa.table("t", sa.column("id"))
    query = sqlite_qb.insert(t, {"id": 1}, return_results=True)
    assert query is not None


def test_sqlite_update_returning(sqlite_qb):
    t = sa.table("t", sa.column("a"))
    query = sqlite_qb.update(t, {"a": 1}, return_results=True)
    assert query is not None


def test_sqlite_delete_returning(sqlite_qb):
    t = sa.table("t")
    query = sqlite_qb.delete(t, return_results=True)
    assert query is not None


def test_postgresql_query_builder_options(postgresql_qb):
    assert postgresql_qb.supports(Option.RETURNING)
    assert postgresql_qb.supports(Option.CONFLICTS)
    assert postgresql_qb.supports(Option.LOCKS)


def test_postgresql_insert_do_nothing(postgresql_qb):
    t = sa.table("t", sa.column("id"))
    on_conflict = OnConflict(do="ignore", options={"index_elements": {"id"}})
    query = postgresql_qb.insert(t, {"id": 1}, on_conflict=on_conflict)
    assert query is not None


def test_postgresql_insert_do_update(postgresql_qb):
    t = sa.table("t", sa.column("id"), sa.column("name"))
    on_conflict = OnConflict(
        do="update", options={"index_elements": {"id"}, "set_": {"name"}}
    )
    query = postgresql_qb.insert(t, {"id": 1, "name": "test"}, on_conflict=on_conflict)
    assert query is not None


def test_postgresql_lock_unlock(postgresql_qb):
    lock = postgresql_qb.lock("test_key")
    unlock = postgresql_qb.unlock("test_key")
    assert lock is not None
    assert unlock is not None


def test_mysql_query_builder_options(mysql_qb):
    assert mysql_qb.supports(Option.RETURNING)
    assert mysql_qb.supports(Option.CONFLICTS)
    assert mysql_qb.supports(Option.LOCKS)


def test_mysql_insert_do_update(mysql_qb):
    t = sa.table("t", sa.column("id"), sa.column("name"))
    on_conflict = OnConflict(do="update", options={"set_": {"name"}})
    query = mysql_qb.insert(t, {"id": 1, "name": "test"}, on_conflict=on_conflict)
    assert query is not None


def test_mysql_upsert_accepts_list_values(mysql_qb):
    t = sa.table("t", sa.column("id"), sa.column("name"))
    on_conflict = OnConflict(do="update", options={"set_": {"name"}})
    query = mysql_qb.insert(
        t, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], on_conflict=on_conflict
    )
    assert query is not None


def test_get_query_builder_unknown_dialect():
    qb = get_query_builder("unknown_dialect")
    assert isinstance(qb, QueryBuilder)
    assert not qb.supports(Option.RETURNING)
