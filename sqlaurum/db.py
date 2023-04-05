from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool, Pool

from .repository import SQLAlchemyModelRepository


def configure_repository_class(dialect):
    if dialect == "postgres":
        from .dialects.postgres import configure_postgres_dialect

        configure_postgres_dialect(SQLAlchemyModelRepository)

    elif dialect == "sqlite":
        from .dialects.sqlite import configure_sqlite_dialect

        configure_sqlite_dialect(SQLAlchemyModelRepository)


class Database:
    def __init__(
        self,
        url: str,
        poolclass: Pool = AsyncAdaptedQueuePool,
        pool_size: int = 10,
        max_overflow: int = 0,
        pool_recycle: int = 1200,
        echo: bool = False,
        echo_pool: bool = True,
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
            **kwargs,
        )

        self.session_maker = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )
        self.session = asynccontextmanager(self.session_factory)
        self.__call__ = self.session_factory

        configure_repository_class(self.engine.url.get_dialect().name)

    async def session_factory(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.session_maker() as session:
            try:
                yield session
                await session.commit()
            except:  # noqa
                await session.rollback()
                raise
