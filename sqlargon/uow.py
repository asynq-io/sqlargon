from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, get_type_hints

from databases.core import Transaction

from sqlargon import Database


class SQLAlchemyUnitOfWork(AbstractAsyncContextManager):
    __slots__ = ("db", "_transaction", "_repositories")

    def __init__(
        self,
        db: Database,
    ) -> None:
        self.db = db
        self._transaction: Transaction | None = None
        self._repositories = {}

    async def __aenter__(self) -> Transaction:
        if self._transaction is not None:
            raise RuntimeError("Unit of work context already opened")
        self._transaction = self.db.transaction()
        assert self._transaction is not None
        await self._transaction.start()
        return self._transaction

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        assert self._transaction is not None
        await self._transaction.__aexit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, item: str) -> Any:
        if item.startswith("__"):
            return self.__getattribute__(item)
        if item not in self._repositories:
            repository_cls = get_type_hints(self).get(item)
            if repository_cls is None:
                raise TypeError("Could not resolve type annotation for %s", item)
            self._repositories[item] = repository_cls(self.db)
        return self._repositories[item]
