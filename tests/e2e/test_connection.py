import sqlalchemy as sa
from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from sqlargon import Database
from sqlargon.query_builder import Option

from .backends import Backend
from .models import TABLES


class _ScopedBase(DeclarativeBase):
    """A metadata of its own, so ``create_all`` here touches nothing else."""

    metadata = MetaData()


class _Widget(_ScopedBase):
    __tablename__ = "e2e_widget"

    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)


class _ScopedDatabase(Database):
    Model = _ScopedBase


async def table_names(db: Database) -> set[str]:
    async with db.engine.connect() as connection:
        return set(
            await connection.run_sync(lambda sync: sa.inspect(sync).get_table_names())
        )


async def test_verify_connection(db: Database):
    await db.verify_connection()


def test_dialect_matches_the_backend(db: Database, backend: Backend):
    assert db.dialect == backend.dialect


def test_query_builder_matches_the_dialect(db: Database, backend: Backend):
    assert db.query_builder.supports(Option.LOCKS) is backend.native_locks


async def test_schema_exists_on_the_server(db: Database):
    assert {table.name for table in TABLES} <= await table_names(db)


async def test_create_all_then_drop_all(database_url: str):
    database = _ScopedDatabase(database_url)
    try:
        await database.create_all()
        assert _Widget.__tablename__ in await table_names(database)
        await database.drop_all()
        assert _Widget.__tablename__ not in await table_names(database)
    finally:
        await database.dispose()


async def test_execute_runs_on_the_server(db: Database):
    result = await db.execute(sa.select(sa.literal(1)))
    assert result.scalar() == 1


async def test_execute_reuses_the_context_local_session(db: Database):
    async with db.session_context() as session:
        await db.execute(sa.select(sa.literal(1)))
        async with db.session_context() as nested:
            assert nested is session
