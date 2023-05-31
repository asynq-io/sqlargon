from typing import Callable, Dict, Type

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import Database
from ..repository import SQLAlchemyModelRepository


class FastapiRepositoryProvider:
    def __init__(self, db: Database):
        self.db = db
        self._wrappers: Dict[
            Type[SQLAlchemyModelRepository],
            Callable[[AsyncSession], SQLAlchemyModelRepository],
        ] = {}

    def __getitem__(
        self, item: Type[SQLAlchemyModelRepository]
    ) -> Callable[[AsyncSession], SQLAlchemyModelRepository]:
        if not issubclass(item, SQLAlchemyModelRepository):
            raise KeyError

        if item not in self._wrappers:

            def wrapped(
                session: AsyncSession = Depends(self.db.session_factory),
            ) -> SQLAlchemyModelRepository:
                return item(session=session)

            self._wrappers[item] = wrapped

        return Depends(self._wrappers[item])
