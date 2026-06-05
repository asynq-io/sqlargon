from __future__ import annotations

from abc import abstractmethod
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from typing import TYPE_CHECKING, Any, get_type_hints

from .registry import db_registry
from .repository import SQLAlchemyRepository

if TYPE_CHECKING:
    from types import TracebackType

    from sqlalchemy.ext.asyncio import AsyncSession

    from .database import Database


class _RepositoryDescriptor:
    def __init__(self, repository_cls: type[SQLAlchemyRepository]) -> None:
        self.repository_cls = repository_cls

    def __get__(
        self, instance: SQLAlchemyUnitOfWork | None, owner: type[SQLAlchemyUnitOfWork]
    ) -> type[SQLAlchemyRepository] | SQLAlchemyRepository:
        if instance is None:
            return self.repository_cls
        repository = self.repository_cls()
        repository.use_db(instance.db_name)
        return repository


class AbstractUnitOfWork(AbstractAsyncContextManager):
    @abstractmethod
    async def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError


class SQLAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        hints = get_type_hints(cls)
        for name, hint in hints.items():
            if (
                isinstance(hint, type)
                and issubclass(hint, SQLAlchemyRepository)
                and not isinstance(cls.__dict__.get(name), _RepositoryDescriptor)
            ):
                setattr(cls, name, _RepositoryDescriptor(hint))

    def __init__(self) -> None:
        self._stack = AsyncExitStack()
        self._session: AsyncSession | None = None
        self._use_db: str | None = None

    @property
    def db_name(self) -> str:
        return self._use_db or "default"

    def use_db(self, name: str) -> None:
        if self._use_db is not None:
            msg = "DB name already set"
            raise ValueError(msg)
        self._use_db = name

    @property
    def db(self) -> Database:
        return db_registry[self.db_name]

    @property
    def session(self) -> AsyncSession:
        if self._session is None:
            msg = "Session not initialized"
            raise ValueError(msg)
        return self._session

    async def __aenter__(self) -> None:
        if self._session is not None:
            msg = "Cannot open the same unit of work more than once"
            raise ValueError(msg)
        await self._stack.__aenter__()
        session = await self._stack.enter_async_context(self.db.session_context())
        self._session = session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._session = None
        await self._stack.__aexit__(exc_type, exc_val, exc_tb)

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
