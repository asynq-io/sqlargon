from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import Lock
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, overload

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql.dml import UpdateBase
from typing_extensions import ParamSpec, Self

from .orm import Base
from .query_builder import Option, get_query_builder
from .routing import RoutingContext

try:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
except ImportError:
    SQLAlchemyInstrumentor = None

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable
    from contextlib import AbstractAsyncContextManager

    from sqlalchemy import Executable, Result
    from sqlalchemy.sql.selectable import TypedReturnsRows

    from .query_builder import QueryBuilder
    from .typing import Params


P = ParamSpec("P")
R = TypeVar("R")


class ReadOnlyError(RuntimeError):
    """Raised when a write statement is executed against a read replica."""


class BaseDatabase(ABC):
    """Shared interface of a single :class:`Database` and a
    :class:`~sqlargon.DatabaseCluster`.

    Everything built purely on sessions -- statement execution, named locks
    and the ``atomic``/``with_lock``/``inject_session`` decorators -- lives
    here, so code can accept either implementation interchangeably.
    """

    Model = Base

    dialect: str
    query_builder: QueryBuilder

    _locks: ClassVar[dict[str, Lock]] = {}

    @abstractmethod
    def route(self, context: RoutingContext | None = None) -> Database:
        """Return the concrete database a statement should run against."""

    @abstractmethod
    def session(
        self, context: RoutingContext | None = None
    ) -> AbstractAsyncContextManager[AsyncSession]:
        """Open a fresh session, committed on success and rolled back on error."""

    @abstractmethod
    def session_context(
        self, context: RoutingContext | None = None
    ) -> AbstractAsyncContextManager[AsyncSession]:
        """Yield the context-local session, opening one if none is active."""

    @abstractmethod
    async def verify_connection(self) -> None:
        """Check every underlying engine can execute a trivial statement."""

    @abstractmethod
    async def dispose(self) -> None:
        """Dispose every underlying engine and its connection pool."""

    @abstractmethod
    async def create_all(self) -> None:
        """Create all tables of :attr:`Model` metadata on writable databases."""

    @abstractmethod
    async def drop_all(self) -> None:
        """Drop all tables of :attr:`Model` metadata on writable databases."""

    async def execute(
        self,
        query: Executable | TypedReturnsRows,
        params: Params | None = None,
        **kwargs: Any,
    ) -> Result[Any]:
        """Execute a statement on the routed context-local session."""
        context = RoutingContext.create(statement=query)
        async with self.session_context(context) as session:
            return await session.execute(query, params, **kwargs)

    @asynccontextmanager
    async def lock(self, name: str) -> AsyncGenerator[None]:
        """Hold a named lock: a native (advisory) lock when the dialect
        supports one, a process-local asyncio lock otherwise."""
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
        """Hold a database-native advisory lock for the duration of the block."""
        _lock, _release = self.query_builder.get_lock_pair(name)
        async with self.session_context() as session:
            try:
                await session.execute(_lock)
                yield
            finally:
                await session.execute(_release)

    def inject_session(
        self, func: Callable[P, Awaitable[R]]
    ) -> Callable[P, Awaitable[R]]:
        """Provide the context-local session as the ``session`` keyword
        argument unless the caller already passed one."""

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
        """Run the decorated coroutine within a single transaction."""

        @wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            async with self.session_context():
                return await fn(*args, **kwargs)

        return wrapper

    @overload
    def with_lock(
        self, fn: Callable[P, Awaitable[R]], *, key: str
    ) -> Callable[P, Awaitable[R]]: ...

    @overload
    def with_lock(
        self, fn: None = None, *, key: str
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]: ...

    def with_lock(
        self,
        fn: Callable[P, Awaitable[R]] | None = None,
        *,
        key: str,
    ) -> (
        Callable[P, Awaitable[R]]
        | Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]
    ):
        """Run the decorated coroutine while holding the named :meth:`lock`."""

        def wrapper(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            @wraps(fn)
            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
                async with self.lock(key):
                    return await fn(*args, **kwargs)

            return wrapped

        if fn is not None:
            return wrapper(fn)
        return wrapper


class Database(BaseDatabase):
    """A single (writable) database: one engine, sessions and query builder.

    Routing policy lives outside this class -- to front replicas or shards,
    compose ``Database`` objects into a :class:`~sqlargon.DatabaseCluster`.
    """

    def __init__(
        self,
        url: str,
        *,
        enable_tracker: bool = False,
        **kwargs: Any,
    ) -> None:
        self._lock = Lock()
        self.engine = create_async_engine(url, **kwargs)
        self.session_maker = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )
        self._current_session: ContextVar[AsyncSession | None] = ContextVar(
            "_current_session", default=None
        )
        self.dialect = self.engine.url.get_dialect().name
        self.query_builder = get_query_builder(self.dialect)

        if enable_tracker:
            from .tracker import TRACKER

            TRACKER.track_pool(self.engine.sync_engine)

        if SQLAlchemyInstrumentor is not None:
            SQLAlchemyInstrumentor().instrument(engine=self.engine.sync_engine)

    def route(self, context: RoutingContext | None = None) -> Database:  # noqa: ARG002
        """Return the database a statement should run against (always ``self``).

        Exists so a single database and a cluster expose the same interface.
        """
        return self

    @asynccontextmanager
    async def session(
        self,
        context: RoutingContext | None = None,  # noqa: ARG002
    ) -> AsyncGenerator[AsyncSession]:
        """Open a fresh session, committed on success and rolled back on error.

        ``context`` is accepted for interface parity with
        :class:`~sqlargon.DatabaseCluster` and ignored.
        """
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
    async def session_context(
        self,
        context: RoutingContext | None = None,  # noqa: ARG002
    ) -> AsyncGenerator[AsyncSession]:
        """Yield the context-local session, opening one if none is active.

        ``context`` is accepted for interface parity with
        :class:`~sqlargon.DatabaseCluster` and ignored -- there is only one
        database to route to.
        """
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

    async def verify_connection(self) -> None:
        async with self.session() as session:
            await session.execute(text("SELECT 1;"))

    async def dispose(self) -> None:
        await self.engine.dispose()

    async def create_all(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Model.metadata.create_all)

    async def drop_all(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(self.Model.metadata.drop_all)

    @classmethod
    def from_env(cls, **kwargs: Any) -> Self:
        """Build a single database from ``DATABASE_*`` settings.

        To route across replicas, build a :class:`~sqlargon.DatabaseCluster`
        with :meth:`~sqlargon.DatabaseCluster.from_env` instead.
        """
        from .settings import DatabaseSettings

        settings = DatabaseSettings(**kwargs)
        return cls(**settings.to_kwargs() | kwargs)


class ReadOnlyDatabase(Database):
    """A read replica that rejects any write statement at the engine level."""

    def __init__(self, url: str, **kwargs: Any) -> None:
        super().__init__(url, **kwargs)
        event.listen(self.engine.sync_engine, "before_execute", self._reject_writes)

    @staticmethod
    def _reject_writes(
        _conn: Any,
        clauseelement: Any,
        _multiparams: Any,
        _params: Any,
        _execution_options: Any,
    ) -> None:
        if isinstance(clauseelement, UpdateBase):
            msg = f"Cannot execute {type(clauseelement).__name__} on a read replica"
            raise ReadOnlyError(msg)
