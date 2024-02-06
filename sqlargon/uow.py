from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import TypeVar, get_type_hints

from sqlalchemy.ext.asyncio import AsyncSession

from sqlargon import Database, SQLAlchemyRepository

U = TypeVar("U", bound="AbstractUnitOfWork")


class AbstractUnitOfWork(ABC):
    @abstractmethod
    async def __aenter__(self: U) -> U:
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        raise NotImplementedError

    @abstractmethod
    async def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError


class SQLAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(
        self,
        db: Database,
        autocommit: bool = True,
        raise_on_exc: bool = True,
    ) -> None:
        self.db = db
        self.autocommit = autocommit
        self.raise_on_exc = raise_on_exc
        self._repositories: dict[str, SQLAlchemyRepository] = {}
        self._session: AsyncSession | None = None

    async def __aenter__(self):
        session = self.db.session()
        self.db.current_session = session

    @property
    def session(self) -> AsyncSession:
        session = self.db.current_session
        if session is None:
            raise ValueError("Session not initialized")
        return session

    async def _close(self, session: AsyncSession, exc: Exception | None = None) -> None:
        # session is passed explicitly because _close is called
        # in another asyncio.task with a different context
        try:
            if exc:
                await session.rollback()
            elif self.autocommit:
                try:
                    await session.commit()
                except:  # noqa
                    await session.rollback()
                    if self.raise_on_exc:
                        raise
        finally:
            await session.close()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        task = asyncio.create_task(self._close(self.session, exc_val))
        await asyncio.shield(task)
        del self.db.current_session

    async def commit(self) -> None:
        try:
            await self.session.commit()
        except:  # noqa
            await self.session.rollback()
            if self.raise_on_exc:
                raise

    async def rollback(self) -> None:
        await self.session.rollback()

    def __getattr__(self, item: str) -> SQLAlchemyRepository:
        if item not in self._repositories:
            repository_cls = get_type_hints(self).get(item)
            if repository_cls is None:
                raise TypeError("Could not resolve type annotation for %s", item)
            self._repositories[item] = repository_cls(self.db)
        return self._repositories[item]
