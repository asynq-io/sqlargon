from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Generator,
    Generic,
    Mapping,
    Sequence,
    TypedDict,
    TypeVar,
)

import sqlalchemy as sa
from sqlalchemy import Executable, Result, ScalarResult
from sqlalchemy.ext.asyncio import AsyncScalarResult, AsyncSession
from typing_extensions import Self

from .orm import ORMModel

if TYPE_CHECKING:
    from sqlalchemy.sql import ClauseElement
    from sqlalchemy.sql._typing import (
        _ColumnExpressionArgument,
        _ColumnsClauseArgument,
        _FromClauseArgument,
        _JoinTargetArgument,
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

    supports_returning: bool = False
    supports_on_conflict: bool = False

    model: type[Model]
    order_by: str | _ColumnExpressionArgument[_T] | None = None
    default_execution_options: tuple[tuple, dict] = ((), {})
    default_page_size: int = 100

    _insert = staticmethod(sa.insert)
    _update = staticmethod(sa.update)
    _delete = staticmethod(sa.delete)
    _select = staticmethod(sa.select)
    _default_set = None

    def __init_subclass__(cls, **kwargs):
        if not (inspect.isabstract(cls) or hasattr(cls, "model")):
            cls.model = cls.__orig_bases__[0].__args__[0]
            assert cls.model, f"Could not resolve model for {cls.__name__}"

    def __init__(
        self,
        session: AsyncSession,
        query: ClauseElement | Callable = None,
    ):
        self._session = session
        self._query = query

    @property
    def raw_query(self) -> str:
        return str(self.query)

    def copy(self, query: ClauseElement | Callable) -> Self:
        return self.__class__(self._session, query)

    def __call__(self, *args, **kwargs) -> Self:
        self._query = self._query(*args, **kwargs)
        return self

    def __getattr__(self, item: str) -> Self:
        self._query = getattr(self._query, item)
        return self

    def __aiter__(self) -> Coroutine[Any, Any, AsyncScalarResult[Any]]:
        return self.session.stream_scalars(self.query)

    def __await__(self) -> Generator[Any, None, Result]:
        return self.execute().__await__()

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
        if self._query is None:
            query = self._select(self.model)
            if type(self).order_by:
                query = query.order_by(type(self).order_by)
            self._query = query
        return self._query

    @property
    def session(self) -> AsyncSession:
        if self._session is None:
            raise AttributeError("Session not set")
        return self._session

    def filter(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> Self:
        query = self._query
        if query is None:
            query = self._select(self.model)

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
        if type(self).order_by is not None:
            query = query.order_by(type(self).order_by)
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

        query = self._insert(self.model).values(values)
        if self.supports_returning and return_results:
            query = query.returning(self.model)
        kwargs.update(**self.on_conflict)
        if set_ is None:
            if isinstance(values, Mapping):
                pk_columns = {c.name for c in self.model.__table__.primary_key.columns}  # type: ignore[attr-defined]
                set_ = {k for k in values.keys() if k not in pk_columns}
            else:
                set_ = self.on_conflict.get("set_", self._get_default_set())
        if set_:
            kwargs["set_"] = {k: getattr(query.excluded, k) for k in set_}
        query = query.on_conflict_do_update(**kwargs)
        return self.copy(query)

    def update(self, values: Any, return_results: bool = False) -> Self:
        query = self._update(self.model).values(values)
        if self.supports_returning and return_results:
            query = query.returning(self.model)
        return self.copy(query)

    def delete(self, return_results: bool = False) -> Self:
        query = self._delete(self.model)
        if self.supports_returning and return_results:
            query = query.returning(self.model)

        return self.copy(query)

    def join(
        self,
        target: _JoinTargetArgument,
        left: _FromClauseArgument,
        right: _FromClauseArgument,
        onclause: _OnClauseArgument | None = None,
        isouter: bool = False,
        full: bool = False,
    ) -> Self:
        query = self._query or self._select(self.model)
        query = query.join(target, left, right, onclause=onclause, isouter=isouter, full=full)  # type: ignore[union-attr]
        return self.copy(query)

    def page(self, n: int = 1, page_size: int | None = None) -> Self:
        page_size = page_size or type(self).default_page_size
        offset = (n - 1) * page_size
        return self.select().offset(offset).limit(page_size)  # type: ignore[return-value]

    async def count(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> int:
        query = self._select(sa.func.count()).select_from(self.model)
        if args:
            query = query.filter(*args)
        if kwargs:
            query = query.filter_by(**kwargs)
        return (await self.execute_query(query)).scalar()

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
    async def transaction(self, nested: bool = True):
        if nested:
            async with self.session.begin_nested():
                yield
        else:
            async with self.session.begin():
                yield

    async def execute_query(self, query: Executable, *args, **kwargs) -> Result:
        if not args or kwargs:
            args, kwargs = type(self).default_execution_options
        return await self.session.execute(query, *args, **kwargs)

    async def execute(self) -> Result:
        args, kwargs = type(self).default_execution_options
        return await self.execute_query(self.query, *args, **kwargs)

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
        return await self.upsert(kwargs, return_results=True).one()

    async def get_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs
    ) -> Model:
        defaults = defaults or {}
        defaults.update(kwargs)
        obj = await self.insert(defaults, ignore_conflicts=True).one_or_none()
        if obj is None:
            obj = await self.filter(**kwargs).one()
        return obj

    async def create(self, **kwargs) -> Model | None:
        return await self.insert(
            kwargs, ignore_conflicts=True, return_results=True
        ).one_or_none()

    async def add(self, obj: Model, flush: bool = True) -> None:
        self.session.add(obj)
        if flush:
            await self.session.flush()

    async def remove(self, *args, **kwargs) -> None:
        await self.delete().filter(*args, **kwargs).execute()

    async def delete_one(self, *args, **kwargs) -> Model | None:
        return (
            await self.delete(return_results=True).filter(*args, **kwargs).one_or_none()
        )

    async def delete_many(self, *args, **kwargs) -> Sequence[Model]:
        return await self.delete(return_results=True).filter(*args, **kwargs).all()

    async def list(self, offset: int | None = None, limit: int | None = None):
        query = self._select(self.model)
        if offset is not None:
            query = query.offset(offset)
        if limit is not None:
            query = query.limit(limit)
        if type(self).order_by is not None:
            query = query.order_by(type(self).order_by)
        self._query = query
        return await self.all()

    async def bulk_create_or_update(
        self,
        values: Sequence[Mapping],
        return_results: bool = False,
        set_: set[str] | None = None,
        **kwargs: Any,
    ) -> Sequence[Model] | None:
        q = self.upsert(values, return_results=return_results, set_=set_, **kwargs)
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
