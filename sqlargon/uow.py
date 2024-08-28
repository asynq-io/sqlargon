from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, get_type_hints

from sqlalchemy.ext.asyncio import AsyncSession

from sqlargon import Database, SQLAlchemyRepository


class AbstractUnitOfWork(ABC):
    @abstractmethod
    async def __aenter__(self) -> None:
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

    @property
    def session(self) -> AsyncSession:
        if self._session is None:
            raise ValueError("Session not initialized")
        return self._session

    async def __aenter__(self) -> None:
        self._session = await self.db.session().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        task = asyncio.create_task(self.session.__aexit__(exc_type, exc_val, exc_tb))
        await asyncio.shield(task)
        self._session = None

    async def commit(self) -> None:
        try:
            await self.session.commit()
        except:  # noqa
            await self.session.rollback()
            if self.raise_on_exc:
                raise

    async def rollback(self) -> None:
        await self.session.rollback()

    def __getattr__(self, item: str) -> Any:
        if item.startswith("__"):
            return self.__getattribute__(item)
        if item not in self._repositories:
            repository_cls = get_type_hints(self).get(item)
            if repository_cls is None:
                raise TypeError("Could not resolve type annotation for %s", item)
            self._repositories[item] = repository_cls(self.db, self.session)
        return self._repositories[item]
