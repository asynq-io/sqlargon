import asyncio
from abc import ABC, abstractmethod

from sqlalchemy.ext.asyncio import AsyncSession


class AbstractUoW(ABC):
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


class SQLAlchemyUnitOfWork(AbstractUoW):
    def __init__(
        self, session_factory, autocommit: bool = True, raise_on_exc: bool = True
    ):
        self.session_factory = session_factory
        self.autocommit = autocommit
        self.raise_on_exc = raise_on_exc

    @property
    def session(self) -> AsyncSession:
        return self._session

    async def __aenter__(self):
        self._session = self.session_factory()
        return self

    async def _close(self, *exc) -> None:
        try:
            if exc:
                await self.session.rollback()
            elif self.autocommit:
                await self.commit()
        finally:
            await self.session.close()

    async def __aexit__(self, *exc):
        task = asyncio.create_task(self._close(*exc))
        await asyncio.shield(task)

    async def commit(self) -> None:
        try:
            await self.session.commit()
        except:  # noqa
            await self.session.rollback()
            if self.raise_on_exc:
                raise

    async def rollback(self) -> None:
        await self.session.rollback()
