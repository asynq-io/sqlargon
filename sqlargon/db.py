from __future__ import annotations

import functools
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from .repository import SQLAlchemyRepository
from .settings import DatabaseSettings
from .uow import SQLAlchemyUnitOfWork
from .util import json_dumps, json_loads

try:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
except ImportError:
    SQLAlchemyInstrumentor = None


def configure_repository_class(dialect: str) -> None:
    if dialect == "postgresql":
        from .dialects.postgres import configure_postgres_dialect

        configure_postgres_dialect(SQLAlchemyRepository)

    elif dialect == "sqlite":
        from .dialects.sqlite import configure_sqlite_dialect

        configure_sqlite_dialect(SQLAlchemyRepository)


class Database:
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
        configure_repository_class(self.engine.url.get_dialect().name)

        if SQLAlchemyInstrumentor is not None:
            SQLAlchemyInstrumentor().instrument(engine=self.engine.sync_engine)

    async def session_factory(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.session_maker() as session:
            try:
                yield session
                await session.commit()
            except:  # noqa
                await session.rollback()
                raise

    @classmethod
    def from_settings(cls, settings: DatabaseSettings):
        return cls(**settings.dict())

    @classmethod
    def from_env(cls, **kwargs):
        settings = DatabaseSettings(**kwargs)
        return cls.from_settings(settings)

    def inject_session(self, func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            if "session" not in kwargs or kwargs["session"] is None:
                async with self.session() as session:
                    kwargs["session"] = session
                    return await func(*args, **kwargs)

        return wrapped

    def inject_repository(
        self, cls: type[SQLAlchemyRepository], name: str = "repository"
    ):
        def wrapper(func):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                if name not in kwargs or kwargs[name] is None:
                    async with self.session() as session:
                        repository = cls(session=session)
                        kwargs[name] = repository
                        return await func(*args, **kwargs)

            return wrapped

        return wrapper

    def inject_uow(self, cls: type[SQLAlchemyUnitOfWork], name: str = "uow"):
        def wrapper(func):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                if name not in kwargs or kwargs[name] is None:
                    instance = cls(self.session_maker)
                    kwargs[name] = instance
                return await func(*args, **kwargs)

            return wrapped

        return wrapper
