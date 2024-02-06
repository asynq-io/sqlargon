from __future__ import annotations

import functools
from collections.abc import AsyncGenerator, Awaitable, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import sqlalchemy as sa
from sqlalchemy import Executable
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from typing_extensions import ParamSpec

from .orm import Base, ORMModel
from .settings import DatabaseSettings
from .utils import json_dumps, json_loads

try:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
except ImportError:
    SQLAlchemyInstrumentor = None

if TYPE_CHECKING:
    from .repository import SQLAlchemyRepository
    from .uow import SQLAlchemyUnitOfWork

P = ParamSpec("P")
R = TypeVar("R")

M = TypeVar("M", bound=ORMModel)


class Database:
    Model = Base
    Column = sa.Column

    supports_returning: bool = False
    supports_on_conflict: bool = False
    default_execution_options: tuple[tuple, dict] = ((), {})

    insert = staticmethod(sa.insert)
    update = staticmethod(sa.update)
    delete = staticmethod(sa.delete)
    select = staticmethod(sa.select)

    def __init__(
        self,
        url: str,
        json_serializer: Callable[[Any], str] = json_dumps,
        json_deserializer: Callable[[str], Any] = json_loads,
        **kwargs: Any,
    ) -> None:
        self.engine = create_async_engine(
            url=url,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
            **kwargs,
        )

        self.session_maker = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )
        self.session = asynccontextmanager(self.session_factory)
        self._current_session: ContextVar[AsyncSession | None] = ContextVar(
            "_current_session", default=None
        )

        dialect = self.engine.url.get_dialect().name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert

            self.insert = insert  # type: ignore[assignment]
            self.supports_returning = True
            self.supports_on_conflict = True

        elif dialect == "sqlite":
            import sqlite3

            from sqlalchemy.dialects.sqlite import insert

            self.insert = insert  # type: ignore[assignment]
            self.supports_returning = sqlite3.sqlite_version > "3.35"
            self.supports_on_conflict = True

        if SQLAlchemyInstrumentor is not None:
            SQLAlchemyInstrumentor().instrument(engine=self.engine.sync_engine)

    async def create_all(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Model.metadata.create_all)

    async def drop_all(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Model.metadata.drop_all)

    @property
    def in_session_context(self) -> bool:
        return self.current_session is not None

    @property
    def current_session(self) -> AsyncSession | None:
        return self._current_session.get()

    @current_session.setter
    def current_session(self, session: AsyncSession | None):
        self._current_session.set(session)

    @current_session.deleter
    def current_session(self):
        self._current_session.set(None)

    async def session_factory(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.session_maker() as session:
            try:
                self.current_session = session
                yield session
                await session.commit()
            except:  # noqa
                await session.rollback()
                raise
            finally:
                await session.close()
                del self.current_session

    @classmethod
    def from_settings(cls, settings: DatabaseSettings):
        return cls(**settings.to_kwargs())

    @classmethod
    def from_env(cls, **kwargs):
        settings = DatabaseSettings(**kwargs)
        return cls.from_settings(settings)

    async def execute(self, query: Executable, *args, **kwargs):
        if not args or kwargs:
            args, kwargs = self.default_execution_options
        if self.current_session:
            return await self.current_session.execute(query, *args, **kwargs)

        async with self.session() as session:
            return await session.execute(query, *args, **kwargs)

    async def execute_many(self, queries: Sequence[Executable], *args, **kwargs):
        if not args or kwargs:
            args, kwargs = self.default_execution_options
        if self.current_session:
            return (
                await self.current_session.execute(query, *args, **kwargs)
                for query in queries
            )
        else:
            async with self.session() as session:
                return (
                    await session.execute(query, *args, **kwargs) for query in queries
                )

    async def scalars(self, query, *args, **kwargs):
        if not args or kwargs:
            args, kwargs = self.default_execution_options
        if self.current_session:
            return (
                await self.current_session.execute(query, *args, **kwargs)
            ).scalars()

        async with self.session() as session:
            return (await session.execute(query, *args, **kwargs)).scalars()

    async def execute_from_connection(self, query: Executable, *args, **kwargs):
        if not args or kwargs:
            args, kwargs = self.default_execution_options
        if self.current_session:
            connection = await self.current_session.connection()
            return await connection.execute(query, *args, **kwargs)

        async with self.session() as session:
            connection = await session.connection()
            return await connection.execute(query, *args, **kwargs)

    async def stream_scalars(self, query, *args, **kwargs):
        if not args or kwargs:
            args, kwargs = self.default_execution_options
        if self.current_session:
            async for row in self._stream_scalars(
                self.current_session, query, *args, **kwargs
            ):
                yield row
        else:
            async with self.session() as session:
                async for row in self._stream_scalars(session, query, *args, **kwargs):
                    yield row

    @staticmethod
    async def _stream_scalars(session, query, *args, **kwargs):
        async with session.stream_scalars(query, *args, **kwargs) as stream:
            async for row in stream:
                yield row

    async def commit(self):
        if self.current_session:
            return await self.current_session.commit()

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

    def Depends(
        self, repository: type[SQLAlchemyRepository], with_session: bool = False
    ) -> Any:
        from fastapi import Depends

        def wrapper():
            return repository(self)

        async def wrapper_with_session():
            async with self.session():
                yield repository(self)

        if with_session:
            return Depends(wrapper_with_session)

        return Depends(wrapper)
