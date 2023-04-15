from typing import Callable, Dict, Generic, Type, TypeVar

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import Database
from ..repository import SQLAlchemyModelRepository

R = TypeVar("R", bound=SQLAlchemyModelRepository)


class FastapiRepositoryProvider(Generic[R]):
    def __init__(self, db: Database):
        self.db = db
        self._wrappers: Dict[Type[R], Callable[[AsyncSession], R]] = {}

    def __getitem__(self, item: Type[R]) -> Callable[[AsyncSession], R]:
        if not issubclass(item, SQLAlchemyModelRepository):
            raise KeyError

        if item not in self._wrappers:

            def wrapped(session: AsyncSession = Depends(self.db.session_factory)) -> R:
                return item(session=session)

            self._wrappers[item] = wrapped

        return self._wrappers[item]
