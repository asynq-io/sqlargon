from __future__ import annotations

import functools
from asyncio import Lock
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import asyncpg
import sqlalchemy as sa
from aiosqlite import register_adapter, register_converter
from databases import Database as AbstractDatabase
from databases import DatabaseURL
from sqlalchemy import Delete, Select, Update
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from typing_extensions import ParamSpec

from .orm import Base
from .settings import DatabaseSettings
from .utils import json_dumps, json_loads

if TYPE_CHECKING:
    from .repository import SQLAlchemyRepository
    from .uow import SQLAlchemyUnitOfWork

P = ParamSpec("P")
R = TypeVar("R")


class Database(AbstractDatabase):
    Model = Base

    select = Select
    update = Update
    delete = Delete
    insert: type[PgInsert | SqliteInsert]

    def __init__(
        self, *, url: DatabaseURL | str, force_rollback: bool = False, **options: Any
    ):
        url = DatabaseURL(url)

        if url.dialect == "postgresql":
            options["init"] = self._init_pg

            self.insert = PgInsert
            self.supports_on_conflict = True
            self.supports_returning = True

        elif url.dialect == "sqlite":
            self.insert = SqliteInsert
        else:
            msg = f"Unsupported dialect {url.dialect}"
            raise ValueError(msg)
        self._lock = Lock()
        dialect_cls = registry.load(url.dialect)
        self.dialect = dialect_cls()

        super().__init__(url, force_rollback=force_rollback, **options)

    def _init_sqlite(self, connection):
        register_adapter(dict, json_dumps)
        register_converter("json", json_loads)

    async def _init_pg(self, connection: asyncpg.Connection):
        # set schema to 'public' ?
        await connection.set_type_codec(
            "jsonb", encoder=json_dumps, decoder=json_loads, schema="pg_catalog"
        )

    async def create_all(self):
        for table in self.Model.metadata.tables.values():
            schema = sa.schema.CreateTable(table, if_not_exists=True)
            query = str(schema.compile(dialect=self.dialect))  # type: ignore
            await self.execute(query=query)

    async def drop_all(self):
        for table in self.Model.metadata.tables.values():
            schema = sa.schema.DropTable(table)
            query = str(schema.compile(dialect=self.dialect))  # type: ignore
            await self.execute(query=query)

    @classmethod
    def from_settings(cls, settings: DatabaseSettings, **kwargs):
        kw = settings.model_dump()
        kw.update(kwargs)
        return cls(**kw)

    @classmethod
    def from_env(cls, **kwargs):
        settings = DatabaseSettings()
        return cls.from_settings(settings, **kwargs)

    def _inject_object(
        self, cls: type[SQLAlchemyRepository] | type[SQLAlchemyUnitOfWork], name: str
    ):
        def wrapper(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            @functools.wraps(func)
            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
                if kwargs.get(name) is None:
                    instance = cls(self)
                    kwargs[name] = instance
                return await func(*args, **kwargs)

            return wrapped

        return wrapper

    def inject_repository(
        self, cls: type[SQLAlchemyRepository], name: str = "repository"
    ):
        return self._inject_object(cls, name)

    def inject_uow(self, cls: type[SQLAlchemyUnitOfWork], name: str = "uow"):
        return self._inject_object(cls, name)

    @asynccontextmanager
    async def lock(self, key: str | int) -> AsyncIterator[None]:
        if self.url.dialect == "postgresql":
            async with self.transaction():
                await self.execute("SELECT pg_advisory_lock(:key)", {"key": key})
                yield
                await self.execute("SELECT pg_advisory_unlock(:key)", {"key": key})
        else:
            async with self._lock:
                yield
