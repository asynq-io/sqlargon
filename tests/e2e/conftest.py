"""Fixtures binding the e2e suite to one real database per backend.

Every test runs once per selected backend. A container is started once per
session and shared, while the :class:`~sqlargon.Database` is per test: an
engine's pool holds connections bound to the event loop that opened them, and
pytest-asyncio gives each test a fresh loop.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from sqlargon import Base, Database

from .backends import Backend, parse_backends
from .models import (
    SERVER_DEFAULT_TABLES,
    TABLES,
    UUIDV7_SERVER_DEFAULT_TABLES,
    DocumentRepository,
    SoftUserRepository,
    UserRepository,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    import sqlalchemy as sa


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Run every e2e test once per selected backend."""
    if "backend" not in metafunc.fixturenames:
        return
    backends = parse_backends(metafunc.config.getoption("e2e_backends"))
    metafunc.parametrize(
        "backend",
        backends,
        ids=[backend.name for backend in backends],
        indirect=True,
        scope="session",
    )


@pytest.fixture(scope="session")
def backend(request: pytest.FixtureRequest) -> Backend:
    selected: Backend = request.param
    pytest.importorskip(selected.driver)
    return selected


@pytest.fixture(scope="session")
def database_url(
    backend: Backend, tmp_path_factory: pytest.TempPathFactory
) -> Generator[str]:
    """Spin the backend up, yield its URL and tear it down after the session."""
    with backend.url(tmp_path_factory.mktemp(f"e2e-{backend.name}")) as url:
        yield url


@pytest.fixture(scope="session")
def tables(backend: Backend) -> tuple[sa.Table, ...]:
    """The e2e tables this backend can hold."""
    tables = TABLES
    if backend.server_side_uuid:
        tables = tables + SERVER_DEFAULT_TABLES
    if backend.server_side_uuidv7:
        tables = tables + UUIDV7_SERVER_DEFAULT_TABLES
    return tables


@pytest.fixture(scope="session")
def schema(database_url: str, tables: tuple[sa.Table, ...]) -> Generator[None]:
    """Create the e2e tables once per backend, and drop them afterwards.

    DDL runs in its own throwaway engine and event loop, so the schema can be
    session scoped without an engine outliving the loop that built it.
    """

    async def run(*, create: bool) -> None:
        engine = create_async_engine(database_url)
        try:
            async with engine.begin() as connection:
                await connection.run_sync(
                    Base.metadata.create_all if create else Base.metadata.drop_all,
                    tables=list(tables),
                )
        finally:
            await engine.dispose()

    asyncio.run(run(create=True))
    yield
    asyncio.run(run(create=False))


@pytest.fixture(autouse=True)
async def db(
    database_url: str, schema: None, tables: tuple[sa.Table, ...]
) -> AsyncGenerator[Database]:
    """The database under test, left with empty tables for the next test."""
    database = Database(database_url)
    try:
        yield database
    finally:
        try:
            async with database.engine.begin() as connection:
                for table in tables:
                    await connection.execute(table.delete())
        finally:
            await database.dispose()


@pytest.fixture
def needs_partial_upsert(backend: Backend) -> None:
    if not backend.partial_upsert:
        pytest.skip(f"{backend.name} rejects an upsert leaving a set column out")


@pytest.fixture
def needs_excluded(backend: Backend) -> None:
    if backend.is_mysql_family:
        pytest.skip(f"QueryBuilder.excluded is unimplemented for {backend.name}")


@pytest.fixture
def needs_native_locks(backend: Backend) -> None:
    if not backend.native_locks:
        pytest.skip(f"{backend.name} has no advisory locks")


@pytest.fixture
def needs_server_side_uuid(backend: Backend) -> None:
    if not backend.server_side_uuid:
        pytest.skip(f"{backend.name} rejects a generated UUID as a column default")


@pytest.fixture
def needs_server_side_uuidv7(backend: Backend) -> None:
    if not backend.server_side_uuidv7:
        pytest.skip(f"{backend.name} lacks uuidv7(), a PostgreSQL 18 server builtin")


@pytest.fixture
def needs_skip_locked(backend: Backend) -> None:
    if not backend.skip_locked:
        pytest.skip(f"{backend.name} has no FOR UPDATE SKIP LOCKED")


@pytest.fixture
def needs_json_key_operators(backend: Backend) -> None:
    if not backend.json_key_operators:
        pytest.skip(f"the {backend.name} JSON key operators match values, not keys")


@pytest.fixture
def users() -> UserRepository:
    return UserRepository()


@pytest.fixture
def documents() -> DocumentRepository:
    return DocumentRepository()


@pytest.fixture
def soft_users() -> SoftUserRepository:
    return SoftUserRepository()
