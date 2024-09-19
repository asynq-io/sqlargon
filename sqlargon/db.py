from __future__ import annotations

import functools
from asyncio import Lock
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from typing_extensions import ParamSpec

from .orm import Base
from .settings import DatabaseSettings
from .tracker import TRACKER
from .utils import json_dumps, json_loads, key_to_int

try:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
except ImportError:
    SQLAlchemyInstrumentor = None

if TYPE_CHECKING:
    from .repository import SQLAlchemyRepository
    from .uow import SQLAlchemyUnitOfWork

P = ParamSpec("P")
R = TypeVar("R")


class Database:
    Model = Base
    Column = sa.Column

    supports_returning: bool = False
    supports_on_conflict: bool = False

    insert = staticmethod(sa.insert)
    update = staticmethod(sa.update)
    delete = staticmethod(sa.delete)
    select = staticmethod(sa.select)

    def __init__(
        self,
        url: str,
        enable_tracker: bool = True,
        json_serializer: Callable[[Any], str] = json_dumps,
        json_deserializer: Callable[[str], Any] = json_loads,
        **kwargs: Any,
    ) -> None:
        self._lock = Lock()
        self.engine = create_async_engine(
            url=url,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
            **kwargs,
        )
        if enable_tracker:
            TRACKER.track_pool(self.engine.pool)

        self.session_maker = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )

        if self.dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert

            self.insert = insert
            self.supports_returning = True
            self.supports_on_conflict = True

        elif self.dialect == "sqlite":
            import sqlite3

            from sqlalchemy.dialects.sqlite import insert

            self.insert = insert
            self.supports_returning = sqlite3.sqlite_version > "3.35"
            self.supports_on_conflict = True

        if SQLAlchemyInstrumentor is not None:
            SQLAlchemyInstrumentor().instrument(engine=self.engine.sync_engine)

    @property
    def dialect(self) -> str:
        return self.engine.url.get_dialect().name

    async def create_all(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Model.metadata.create_all)

    async def drop_all(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Model.metadata.drop_all)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        session = self.session_maker()
        try:
            yield session
            await session.commit()
        except:  # noqa
            await session.rollback()
            raise
        finally:
            await session.close()

    async def execute(self, statement, *args, **kwargs):
        async with self.session() as session:
            return await session.execute(statement, *args, **kwargs)

    @classmethod
    def from_settings(cls, settings: DatabaseSettings):
        return cls(**settings.to_kwargs())

    @classmethod
    def from_env(cls, **kwargs):
        settings = DatabaseSettings(**kwargs)
        return cls.from_settings(settings)

    def inject_session(
        self, func: Callable[P, Awaitable[R]]
    ) -> Callable[P, Awaitable[R]]:
        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if kwargs.get("session") is None:
                async with self.session() as session:
                    kwargs["session"] = session
                    return await func(*args, **kwargs)
            else:
                return await func(*args, **kwargs)

        return wrapped

    def _inject_object(
        self,
        cls: type[SQLAlchemyRepository] | type[SQLAlchemyUnitOfWork],
        name: str,
        **kw: Any,
    ):
        def wrapper(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            @functools.wraps(func)
            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
                if kwargs.get(name) is None:
                    instance = cls(self, **kw)
                    kwargs[name] = instance
                return await func(*args, **kwargs)

            return wrapped

        return wrapper

    def inject_repository(
        self,
        cls: type[SQLAlchemyRepository],
        name: str = "repository",
        provide_session: bool = False,
    ):
        return self._inject_object(cls, name)

    def inject_uow(
        self,
        cls: type[SQLAlchemyUnitOfWork],
        name: str = "uow",
        *,
        raise_on_exc: bool = True,
    ):
        return self._inject_object(cls, name, raise_on_exc=raise_on_exc)

    @asynccontextmanager
    async def lock(self, key: str):
        async with self._lock:
            if self.dialect == "postgresql":
                key_int = key_to_int(key)
                async with self.session() as session:
                    await session.execute(
                        sa.text("SELECT pg_advisory_lock(:key)"), {"key": key_int}
                    )
                    yield
                    await session.execute(
                        sa.text("SELECT pg_advisory_unlock(:key)"), {"key": key_int}
                    )
            else:
                yield
