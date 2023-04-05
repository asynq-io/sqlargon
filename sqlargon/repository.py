from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Generic, Sequence, TypeVar

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import TypedDict

from .orm import ORMModel

Model = TypeVar("Model", bound=ORMModel)


class OnConflict(TypedDict, total=False):
    index_elements: Any | None
    index_where: Any | None
    set_: set[str] | None
    where: Any | None


class BaseSQLAlchemyRepository:
    supports_returning: bool = False

    _insert = staticmethod(sa.insert)
    _update = staticmethod(sa.update)
    _delete = staticmethod(sa.delete)
    _select = staticmethod(sa.select)

    def __init__(self, session: AsyncSession | None = None):
        self._session = session
        self._stmt: Any = None

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return f"<{type(self)}[{self.model}]>"

    def __aiter__(self):
        return self.session.stream_scalars(self._stmt)

    def __getattr__(self, item):
        self._stmt = getattr(self._stmt, item)
        return self

    def __call__(self, *args, **kwargs):
        self._stmt = self._stmt(*args, **kwargs)
        return self

    def __await__(self):
        return self.session.execute(self._stmt).__await__()

    @property
    def session(self) -> AsyncSession:
        assert self._session, "Session not set"
        return self._session

    async def flush(self, objects=None) -> None:
        await self.session.flush(objects)

    async def commit(self, raise_on_exception: bool = True) -> None:
        try:
            await self.session.commit()
        except:  # noqa
            await self.session.rollback()
            if raise_on_exception:
                raise

    @asynccontextmanager
    async def transaction(self):
        async with self.session.begin():
            yield

    async def execute(self, *args, **kwargs):
        if len(args) == 0:
            args = (self._stmt,)
        return await self.session.execute(*args, **kwargs)

    async def scalars(self, *args, **kwargs):
        return (await self.session.execute(self._stmt, *args, **kwargs)).scalars()

    async def all(self, *args, **kwargs):
        return (await self.scalars(*args, **kwargs)).all()

    async def one(self, *args, **kwargs) -> Model:
        return (await self.scalars(*args, **kwargs)).one()

    async def one_or_none(self, *args, **kwargs) -> Model | None:
        return (await self.scalars(*args, **kwargs)).one_or_none()

    get = one_or_none

    async def mappings(self, *args, **kwargs):
        return (await self.session.execute(*args, **kwargs)).mappings()

    def update(self, table, values=None, return_results: bool = True, **kwargs):
        query = self._update(table)
        if self.supports_returning and return_results:
            query = query.returning(table)
        if values:
            query = query.values(values, **kwargs)
        self._stmt = query
        return self

    def insert(
        self,
        table=None,
        values=None,
        return_results: bool = True,
        **kwargs,
    ):
        query = self._insert(table)
        if values:
            query = query.values(values)
        if self.supports_returning and return_results:
            query = query.returning(table)
        self._stmt = query
        return self

    def delete(self, table, *args, **kwargs):
        query = self._delete(table)
        if args:
            query = query.filter(*args)
        if kwargs:
            query = query.filter_by(**kwargs)
        self._stmt = query
        return self

    def select(self, *args, **kwargs):
        self._stmt = self._select(*args, **kwargs)
        return self


class SQLAlchemyModelRepository(BaseSQLAlchemyRepository, Generic[Model]):
    """Base class which provides both SQLAlchemy core (with bound model) and session interfaces"""

    supports_on_conflict: bool = False

    model: Model
    order_by: Any | None = None

    def __init_subclass__(cls, **kwargs):
        if "abstract" not in kwargs:
            cls.model = cls.__orig_bases__[0].__args__[0]
            assert cls.model, f"Could not resolve model for {cls}"

    @property
    def on_conflict(self) -> OnConflict:
        pk_columns = {c.name for c in self.model.__table__.primary_key.columns}  # type: ignore
        return {
            "index_elements": list(pk_columns),
            "set_": {
                c.name for c in self.model.__table__.columns if c.name not in pk_columns  # type: ignore
            },
        }

    async def execute(self, *args, **kwargs):
        if self._stmt is not None:
            args = (self._stmt, *args)
        return await super().execute(*args, **kwargs)

    async def scalars(self, *args, **kwargs):
        if self._stmt is None:
            query = self._select(self.model)
            if self.order_by is not None:
                query = query.order_by(self.order_by)
            self._stmt = query
        return await super().scalars(*args, **kwargs)

    def filter(self, *args, **kwargs):
        if self._stmt is None:
            query = self._select(self.model)
            if self.order_by is not None:
                query = query.order_by(self.order_by)
            query = query.filter(*args).filter_by(**kwargs)
            self._stmt = query
        return self

    async def list(self, *args, **kwargs) -> Sequence[Any]:
        return await self.filter(*args, **kwargs).all()

    def update(self, values=None, **kwargs):  # type: ignore
        return super().update(self.model, values, **kwargs)

    def insert(self, values=None, return_results: bool = True, ignore_conflicts: bool = False, index_where=None, **kwargs):  # type: ignore[override]
        super().insert(self.model, values, return_results=return_results, **kwargs)

        if self.supports_on_conflict and ignore_conflicts:
            self._stmt = self._stmt.on_conflict_do_nothing(
                index_elements=self.on_conflict["index_elements"],
                index_where=index_where,
            )
        return self

    create = insert

    def delete(self, *args, **kwargs):
        return super().delete(self.model, *args, **kwargs)

    def select(self, *args, **kwargs):
        if len(args) == 0:
            args = (self.model,)
        self._stmt = self._select(*args, **kwargs)
        return self

    def upsert(
        self,
        values,
        return_result: bool = True,
        set_: set[str] | None = None,
        **kwargs,
    ):
        assert type(
            self
        ).supports_on_conflict, f"{type(self).__name__} does not support upsert"
        self.insert(values, return_result=return_result)
        kwargs.update(**self.on_conflict)
        set_ = set_ or self.on_conflict["set_"]
        if set_:
            kwargs["set_"] = {k: getattr(self._stmt.excluded, k) for k in set_}
        self._stmt = self._stmt.on_conflict_do_update(**kwargs)
        return self

    async def paginate(self, *args, offset: int = 0, limit: int = 100, **kwargs):
        return await self.filter(*args, **kwargs).offset(offset).limit(limit).all()

    async def retrieve(self, *args, **kwargs) -> Model | None:
        return await self._filter(*args, **kwargs).one_or_none()

    partial_update = update
