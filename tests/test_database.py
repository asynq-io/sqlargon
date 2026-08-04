import asyncio
import sqlite3
from datetime import datetime, timezone

import pytest
import sqlalchemy as sa

from sqlargon import Database
from sqlargon.query_builder import Option, QueryBuilder
from sqlargon.routing import RoutingContext
from sqlargon.settings import DatabaseSettings
from sqlargon.utils import utc_now
from tests import MEMORY_URL


class LockingQueryBuilder(QueryBuilder):
    """A builder advertising native locks, with statements SQLite can run."""

    supported_options = Option.LOCKS

    def lock(self, key: str) -> sa.TextClause:
        return sa.text("SELECT :key AS acquired").bindparams(key=key)

    def unlock(self, key: str) -> sa.TextClause:
        return sa.text("SELECT :key AS released").bindparams(key=key)


@pytest.fixture
def executed_statements(db: Database):
    statements: list[str] = []

    def before_execute(_conn, clauseelement, _multiparams, _params, _options):
        statements.append(str(clauseelement))

    sa.event.listen(db.engine.sync_engine, "before_execute", before_execute)
    yield statements
    sa.event.remove(db.engine.sync_engine, "before_execute", before_execute)


@pytest.fixture
def locking_query_builder(db: Database, monkeypatch):
    monkeypatch.setattr(db, "query_builder", LockingQueryBuilder())
    return db.query_builder


def test_create_database(db: Database):
    assert isinstance(db, Database)


def test_database_dialect(db: Database):
    assert db.dialect == "sqlite"


async def test_create_all_drop_all(db: Database):
    await db.create_all()
    await db.drop_all()


def test_route_always_returns_self(db: Database):
    # interface parity with DatabaseCluster: a single database routes to itself
    assert db.route() is db
    assert db.route(RoutingContext.create(read_only=True)) is db


async def test_session(db: Database):
    async with db.session() as session:
        result = await session.execute(sa.text("SELECT 1"))
        assert result.scalar() == 1


async def test_session_rollback_on_error(db: Database):
    msg = "test"
    with pytest.raises(ValueError, match=msg):
        async with db.session():
            raise ValueError(msg)


async def test_session_context_reuses_session(db: Database):
    async with db.session_context() as s1, db.session_context() as s2:
        assert s1 is s2


async def test_execute(db: Database, user_model):
    await db.create_all()
    try:
        result = await db.execute(sa.select(sa.func.count()).select_from(user_model))
        assert result.scalar() == 0
    finally:
        await db.drop_all()


async def test_lock(db: Database):
    async with db.lock("test"):
        pass


def test_from_env():
    db = Database.from_env()
    assert isinstance(db, Database)
    assert db.dialect == "sqlite"


def test_from_settings():
    settings = DatabaseSettings(url=MEMORY_URL)
    db = Database(**settings.to_kwargs())
    assert isinstance(db, Database)
    assert db.dialect == "sqlite"


async def test_inject_session(db: Database):
    async def my_func(*, session=None):
        return session is not None

    wrapped = db.inject_session(my_func)
    assert await wrapped() is True


async def test_inject_session_preserves_existing(db: Database):
    sentinel = object()

    async def my_func(*, session=None):
        return session

    wrapped = db.inject_session(my_func)
    result = await wrapped(session=sentinel)
    assert result is sentinel


async def test_atomic(db: Database):
    called = False

    async def my_func():
        nonlocal called
        called = True

    wrapped = db.atomic(my_func)
    await wrapped()
    assert called


def test_sqlite_version():
    assert sqlite3.sqlite_version > "3.35"


async def test_lock_uses_process_local_lock_on_sqlite(db: Database):
    async with db.lock("chunk"):
        assert db._locks["chunk"].lock.locked()
    assert "chunk" not in db._locks


async def test_locks_are_per_instance():
    # previously a class variable: every database shared one dict of locks
    a = Database(MEMORY_URL)
    b = Database(MEMORY_URL)
    assert a._locks is not b._locks


async def test_lock_serializes_tasks_under_one_name(db: Database):
    order: list[str] = []

    async def work(tag: str) -> None:
        async with db.lock("serialized"):
            order.append(f"enter {tag}")
            await asyncio.sleep(0.01)
            order.append(f"exit {tag}")

    await asyncio.gather(work("a"), work("b"), work("c"))

    # every enter is immediately followed by the matching exit
    assert [entry.split()[0] for entry in order] == ["enter", "exit"] * 3
    assert [entry.split()[1] for entry in order[::2]] == [
        entry.split()[1] for entry in order[1::2]
    ]


async def test_session_context_recovers_when_session_creation_fails(
    db: Database, monkeypatch
):
    def broken_session_maker():
        msg = "boom"
        raise RuntimeError(msg)

    monkeypatch.setattr(db, "session_maker", broken_session_maker)
    with pytest.raises(RuntimeError, match="boom"):
        async with db.session_context():
            pass  # pragma: no cover
    assert db._current_session.get() is None

    monkeypatch.undo()
    async with db.session_context() as session:
        assert session is not None


@pytest.mark.usefixtures("locking_query_builder")
async def test_lock_uses_native_lock_when_dialect_supports_it(
    db: Database, executed_statements
):
    async with db.lock("native-chunk"):
        assert any("acquired" in statement for statement in executed_statements)
        assert not any("released" in statement for statement in executed_statements)
        assert "native-chunk" not in db._locks
    assert any("released" in statement for statement in executed_statements)


@pytest.mark.usefixtures("locking_query_builder")
async def test_native_lock_releases_on_error(db: Database, executed_statements):
    msg = "boom"
    with pytest.raises(ValueError, match=msg):
        async with db.native_lock("chunk"):
            raise ValueError(msg)
    assert any("acquired" in statement for statement in executed_statements)
    assert any("released" in statement for statement in executed_statements)


async def test_with_lock_as_decorator_factory(db: Database):
    held = []

    @db.with_lock(key="task")
    async def work():
        held.append("task" in db._locks)
        return "done"

    assert await work() == "done"
    assert held == [True]
    assert work.__name__ == "work"
    assert "task" not in db._locks


async def test_with_lock_called_directly(db: Database):
    async def work():
        return "task" in db._locks

    wrapped = db.with_lock(work, key="task")
    assert await wrapped() is True
    assert "task" not in db._locks


async def test_with_lock_releases_on_error(db: Database):
    msg = "boom"

    @db.with_lock(key="task")
    async def work():
        raise ValueError(msg)

    with pytest.raises(ValueError, match=msg):
        await work()
    assert "task" not in db._locks


def test_utc_now_is_timezone_aware():
    before = datetime.now(tz=timezone.utc)
    value = utc_now()
    assert value.tzinfo is timezone.utc
    assert before <= value <= datetime.now(tz=timezone.utc)
