from __future__ import annotations

from asyncio import Lock
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import cached_property, wraps
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from typing_extensions import ParamSpec, Self

from .orm import Base
from .query_builder import Option, get_query_builder
from .registry import db_registry
from .utils import json_dumps, json_loads

try:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
except ImportError:
    SQLAlchemyInstrumentor = None

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from sqlalchemy import Executable, Result
    from sqlalchemy.sql.selectable import TypedReturnsRows

    from .typing import Params


P = ParamSpec("P")
R = TypeVar("R")


class Database:
    Model = Base

    _locks: ClassVar[dict[str, Lock]] = {}

    def __init__(
        self,
        url: str,
        *,
        name: str = "default",
        enable_tracker: bool = False,
        **kwargs: Any,
    ) -> None:
        self._lock = Lock()
        self.name = name
        self.engine = create_async_engine(
            url,
            json_serializer=json_dumps,
            json_deserializer=json_loads,
            **kwargs,
        )

        self.session_maker = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )
        self._current_session: ContextVar[AsyncSession | None] = ContextVar(
            "_current_session", default=None
        )
        self.query_builder = get_query_builder(self.dialect)

        if enable_tracker:
            from .tracker import TRACKER

            TRACKER.track_pool(self.engine.sync_engine)

        if SQLAlchemyInstrumentor is not None:
            SQLAlchemyInstrumentor().instrument(engine=self.engine.sync_engine)

        db_registry.register(self)

    @cached_property
    def dialect(self) -> str:
        return self.engine.url.get_dialect().name

    async def create_all(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Model.metadata.create_all)

    async def drop_all(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Model.metadata.drop_all)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        session = self.session_maker()

        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise
        finally:
            await session.close()

    @asynccontextmanager
    async def session_context(self) -> AsyncGenerator[AsyncSession]:
        await self._lock.acquire()
        current_session = self._current_session.get()

        if current_session is not None:
            self._lock.release()
            yield current_session
        else:
            async with self.session() as session:
                token = self._current_session.set(session)
                self._lock.release()
                try:
                    yield session
                finally:
                    self._current_session.reset(token)

    async def execute(
        self,
        query: Executable | TypedReturnsRows,
        params: Params | None = None,
        **kwargs: Any,
    ) -> Result[Any]:
        async with self.session_context() as session:
            return await session.execute(query, params, **kwargs)

    @asynccontextmanager
    async def lock(self, name: str) -> AsyncGenerator[None]:
        if self.query_builder.supports(Option.LOCKS):
            async with self.native_lock(name):
                yield
        else:
            self._locks.setdefault(name, Lock())
            lock = self._locks[name]
            try:
                async with lock:
                    yield
            finally:
                self._locks.pop(name, None)

    @asynccontextmanager
    async def native_lock(self, name: str) -> AsyncGenerator[None]:
        _lock, _release = self.query_builder.get_lock_pair(name)
        async with self.session_context() as session:
            try:
                await session.execute(_lock)
                yield
            finally:
                await session.execute(_release)

    @classmethod
    def from_env(cls, **kwargs: Any) -> Self:
        from .settings import DatabaseSettings

        settings = DatabaseSettings(**kwargs)
        kw = settings.to_kwargs() | kwargs
        return cls(**kw)

    def inject_session(
        self, func: Callable[P, Awaitable[R]]
    ) -> Callable[P, Awaitable[R]]:

        @wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if kwargs.get("session") is None:
                async with self.session_context() as session:
                    kwargs["session"] = session
                    return await func(*args, **kwargs)
            else:
                return await func(*args, **kwargs)

        return wrapped

    def atomic(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            async with self.session_context():
                return await fn(*args, **kwargs)

        return wrapper

    @overload
    def with_lock(
        self, fn: Callable[P, Awaitable[R]], *, name: str | None = None
    ) -> Callable[P, Awaitable[R]]: ...

    @overload
    def with_lock(
        self, fn: None = None, *, name: str | None = None
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]: ...

    def with_lock(
        self,
        fn: Callable[P, Awaitable[R]] | None = None,
        *,
        name: str | None = None,
    ) -> (
        Callable[P, Awaitable[R]]
        | Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]
    ):
        lock_name = name or str(uuid4())

        def wrapper(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
                async with self.lock(lock_name):
                    return await fn(*args, **kwargs)

            return wrapped

        if fn is not None:
            return wrapper(fn)
        return wrapper
