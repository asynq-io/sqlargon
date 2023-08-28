from typing import Any, Callable, Dict, Type

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import Database
from ..repository import SQLAlchemyRepository


class FastapiRepositoryProvider:
    def __init__(self, db: Database):
        self.db = db
        self._wrappers: Dict[
            Type[SQLAlchemyRepository],
            Callable[[AsyncSession], SQLAlchemyRepository],
        ] = {}

    def __getitem__(self, item: Type[SQLAlchemyRepository]) -> Any:
        if not issubclass(item, SQLAlchemyRepository):
            raise KeyError

        if item not in self._wrappers:

            def wrapped(
                session: AsyncSession = Depends(self.db.session_factory),
            ) -> SQLAlchemyRepository:
                return item(session)

            self._wrappers[item] = wrapped

        return Depends(self._wrappers[item])
