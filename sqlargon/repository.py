from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Mapping,
    Sequence,
    TypedDict,
    TypeVar,
)

import sqlalchemy as sa
from sqlalchemy import Executable, Result, ScalarResult
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import Self

from .orm import ORMModel

if TYPE_CHECKING:
    from sqlalchemy.sql import ClauseElement
    from sqlalchemy.sql._typing import (
        _ColumnExpressionArgument,
        _ColumnsClauseArgument,
        _FromClauseArgument,
        _OnClauseArgument,
    )

Model = TypeVar("Model", bound=ORMModel)
D = TypeVar("D", bound=Any)
_T = TypeVar("_T", bound=Any)


class OnConflict(TypedDict, total=False):
    index_elements: Any | None
    index_where: Any | None
    set_: set[str] | None
    where: Any | None


class SQLAlchemyRepository(Generic[Model]):
    __slots__ = ("_query", "_session", "_options")

    supports_returning: bool = False
    supports_on_conflict: bool = False

    model: type[Model]
    order_by: str | _ColumnExpressionArgument[_T] | None = None
    default_execution_options: tuple[tuple, dict] = ((), {})

    _insert = staticmethod(sa.insert)
    _update = staticmethod(sa.update)
    _delete = staticmethod(sa.delete)
    _select = staticmethod(sa.select)
    _default_set = None

    def __init_subclass__(cls, **kwargs):
        if not inspect.isabstract(cls):
            cls.model = cls.__orig_bases__[0].__args__[0]
            assert cls.model, f"Could not resolve model for {cls.__name__}"

    def __init__(
        self,
        session: AsyncSession,
        query: ClauseElement | Callable = None,
        _options: tuple[tuple, dict] | None = None,
    ):
        self._session = session
        self._query = query
        self._options = _options

    def with_options(self, *args, **kwargs) -> Self:
        self._options = (args, kwargs)
        return self

    @property
    def raw_query(self) -> str:
        return str(self.query)

    def copy(self, query: ClauseElement | Callable) -> Self:
        return self.__class__(self._session, query, self._options)

    def __call__(self, *args, **kwargs) -> Self:
        query = self._query(*args, **kwargs)
        return self.copy(query)

    def __getattr__(self, item: str) -> Self:
        query = getattr(self._query, item)
        return self.copy(query)

    def __aiter__(self):
        return self.session.stream_scalars(self.query)

    @classmethod
    def _get_default_set(cls) -> set[str]:
        if cls._default_set is None:
            pk_columns = {c.name for c in cls.model.__table__.primary_key.columns}
            cls._default_set = {
                c.name for c in cls.model.__table__.columns if c.name not in pk_columns
            }

        return cls._default_set

    @property
    def on_conflict(self) -> OnConflict:
        return {
            "index_elements": [
                c.name for c in self.model.__table__.primary_key.columns  # type: ignore[attr-defined]
            ],
            "set_": self._get_default_set(),
        }

    @property
    def query(self) -> ClauseElement | Executable:
        return self._query

    @property
    def session(self) -> AsyncSession:
        if self._session is None:
            raise AttributeError("Session not set")
        return self._session

    def filter(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> Self:
        query = self._query or self._select(self.model)
        if args:
            query = query.filter(*args)  # type: ignore[union-attr]
        if kwargs:
            query = query.filter_by(**kwargs)  # type: ignore[union-attr]
        return self.copy(query)

    def where(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> Self:
        return self.filter(*args, **kwargs)

    def select(self, *args: _ColumnsClauseArgument) -> Self:
        if len(args) == 0:
            args = (self.model,)
        query = self._select(*args)
        if self.order_by:
            query = query.order_by(self.order_by)
        return self.copy(query)

    def insert(
        self,
        values: Any,
        return_results: bool = True,
        ignore_conflicts: bool = False,
        index_where: _ColumnExpressionArgument[bool] | None = None,
    ) -> Self:
        query = self._insert(self.model).values(values)
        if self.supports_returning and return_results:
            query = query.returning(self.model)

        if self.supports_on_conflict and ignore_conflicts:
            query = query.on_conflict_do_nothing(
                index_elements=self.on_conflict["index_elements"],
                index_where=index_where,
            )

        return self.copy(query)

    def upsert(
        self,
        values: Any,
        return_results: bool = False,
        set_: set[str] | None = None,
        **kwargs: Any,
    ) -> Self:
        if not self.supports_on_conflict:
            raise TypeError("Upsert is not supported")

        query = self.insert(values, return_results).query
        kwargs.update(**self.on_conflict)
        set_ = set_ or self.on_conflict["set_"] or self._get_default_set()
        kwargs["set_"] = {k: getattr(query.excluded, k) for k in set_}
        query = query.on_conflict_do_update(**kwargs)
        return self.copy(query)

    def delete(self, return_results: bool = False) -> Self:
        query = self._delete(self.model)
        if self.supports_returning and return_results:
            query = query.returning(self.model)

        return self.copy(query)

    def join(
        self,
        left: _FromClauseArgument,
        right: _FromClauseArgument,
        onclause: _OnClauseArgument | None = None,
        isouter: bool = False,
        full: bool = False,
    ) -> Self:
        query = self._query or self._select(self.model)
        query = query.join(left, right, onclause=onclause, isouter=isouter, full=full)  # type: ignore[union-attr]
        return self.copy(query)

    async def count(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> int:
        query = self._select(sa.func.count()).select_from(self.model)
        if args:
            query = query.filter(*args)
        if kwargs:
            query = query.filter_by(**kwargs)
        return (await self.execute_raw(query)).scalar()

    async def flush(self, objects: Sequence | None = None) -> None:
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

    async def execute_raw(self, query: Executable, *args, **kwargs) -> Result:
        return await self.session.execute(query, *args, **kwargs)

    async def execute(self) -> Result:
        args, kwargs = self._options or self.default_execution_options
        return await self.execute_raw(self.query, *args, **kwargs)

    async def scalar(self) -> Any:
        return (await self.execute()).scalar()

    async def scalars(self) -> ScalarResult:
        return (await self.execute()).scalars()

    async def all(self) -> Sequence[Model]:
        return (await self.scalars()).all()

    async def one(self) -> Model:
        return (await self.scalars()).one()

    async def one_or_none(self) -> Model | None:
        return (await self.scalars()).one_or_none()

    async def one_or(self, default: D | None = None) -> Model | D | None:
        return (await self.one_or_none()) or default

    async def first(self) -> Model:
        return (await self.scalars()).first()

    async def get(self, *args, **kwargs) -> Model | None:
        return await self.filter(*args, **kwargs).one_or_none()

    async def create_or_update(self, **kwargs) -> Model:
        return await self.upsert(kwargs).one()

    async def get_or_create(self, **kwargs) -> Model:
        return await self.insert(kwargs, ignore_conflicts=True).one()

    async def create(self, **kwargs) -> Model:
        return await self.insert(kwargs).one()

    async def save(self, obj: Model) -> None:
        values = {
            c.key: getattr(obj, c.key) for c in sa.inspect(obj).mapper.column_attrs
        }
        await self.upsert(values, return_results=False).execute()

    async def list(self, offset: int | None = None, limit: int | None = None):
        query = self.query
        if offset is not None:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)
        return await (self.copy(query)).all()

    def paginate(self, limit: int = 100, offset: int = 0) -> Self:
        query = self.query.limit(limit).offset(offset)
        return self.copy(query)

    async def bulk_create_or_update(
        self, values: Sequence[Mapping], return_results: bool = False
    ) -> Sequence[Model] | None:
        q = self.upsert(values, return_results=return_results)
        if return_results:
            return await q.all()
        else:
            await q.execute()
        return None

    async def bulk_create(
        self,
        values: Sequence[Mapping],
        ignore_conflicts: bool = True,
        return_results: bool = False,
    ) -> Sequence[Model] | None:
        q = self.insert(
            values, ignore_conflicts=ignore_conflicts, return_results=return_results
        )
        if return_results:
            return await q.all()
        else:
            await q.execute()
        return None
