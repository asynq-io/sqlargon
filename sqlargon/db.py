import functools
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Type

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool, Pool

from .repository import SQLAlchemyModelRepository
from .settings import DatabaseSettings
from .util import json_dumps, json_loads


def configure_repository_class(dialect):
    if dialect == "postgresql":
        from .dialects.postgres import configure_postgres_dialect

        configure_postgres_dialect(SQLAlchemyModelRepository)

    elif dialect == "sqlite":
        from .dialects.sqlite import configure_sqlite_dialect

        configure_sqlite_dialect(SQLAlchemyModelRepository)


class Database:
    def __init__(
        self,
        url: str,
        poolclass: Type[Pool] = AsyncAdaptedQueuePool,
        pool_size: int = 10,
        max_overflow: int = 0,
        pool_recycle: int = 1200,
        echo: bool = False,
        echo_pool: bool = True,
        json_serializer: Callable[[Any], str] = json_dumps,
        json_deserializer: Callable[[str], Any] = json_loads,
        use_depends: bool = False,
        **kwargs: Any,
    ) -> None:
        self.engine = create_async_engine(
            url=url,
            poolclass=poolclass,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
            echo_pool=echo_pool,
            echo=echo,
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
            **kwargs,
        )

        self.session_maker = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )
        self.session = asynccontextmanager(self.session_factory)
        configure_repository_class(self.engine.url.get_dialect().name)

        if use_depends:
            from .integrations.fastapi import fastapi_integration

            fastapi_integration(self.session_factory)

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

    def inject_session(self):
        def wrapper(func):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                if "session" not in kwargs or kwargs["session"] is None:
                    async with self.session() as session:
                        kwargs["session"] = session
                        return await func(*args, **kwargs)

            return wrapped

        return wrapper

    def inject_repository(
        self, repository_type: Type[SQLAlchemyModelRepository], name: str = "repository"
    ):
        def wrapper(func):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                if name not in kwargs or kwargs[name] is None:
                    async with self.session() as session:
                        repository = repository_type(session=session)
                        kwargs[name] = repository
                        return await func(*args, **kwargs)

            return wrapped

        return wrapper
