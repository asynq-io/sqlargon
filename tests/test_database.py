import sqlite3

import pytest
import sqlalchemy as sa

from sqlargon import Database
from sqlargon.registry import db_registry
from sqlargon.settings import DatabaseSettings


def test_create_database(db: Database):
    assert isinstance(db, Database)


def test_database_dialect(db: Database):
    assert db.dialect == "sqlite"


async def test_create_all_drop_all(db: Database):
    await db.create_all()
    await db.drop_all()


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
    db = Database.from_env(name="from_env_test")
    try:
        assert isinstance(db, Database)
        assert db.dialect == "sqlite"
    finally:
        db_registry.unregister("from_env_test")


def test_from_settings():
    settings = DatabaseSettings(
        url="sqlite+aiosqlite:///:memory:", name="from_settings_test"
    )
    db = Database(**settings.to_kwargs())
    try:
        assert isinstance(db, Database)
        assert db.dialect == "sqlite"
    finally:
        db_registry.unregister("from_settings_test")


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
