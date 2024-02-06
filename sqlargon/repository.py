from __future__ import annotations

import inspect
from collections.abc import Coroutine, Generator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Callable, Generic, TypedDict, TypeVar

import sqlalchemy as sa
from sqlalchemy import Executable, MappingResult, Result, ScalarResult, bindparam
from sqlalchemy.ext.asyncio import AsyncScalarResult
from sqlalchemy.orm import joinedload, load_only
from typing_extensions import Self

from .orm import ORMModel

if TYPE_CHECKING:
    from sqlalchemy.sql import ClauseElement
    from sqlalchemy.sql._typing import (
        _ColumnExpressionArgument,
        _ColumnsClauseArgument,
        _JoinTargetArgument,
        _OnClauseArgument,
    )

    from . import Database

Model = TypeVar("Model", bound=ORMModel)
D = TypeVar("D", bound=Any)
_T = TypeVar("_T", bound=Any)


class OnConflict(TypedDict, total=False):
    index_elements: Any | None
    constraint: str | None
    index_where: Any | None
    set_: set[str] | None
    where: Any | None


class SQLAlchemyRepository(Generic[Model]):
    model: type[Model]
    default_order_by: str | _ColumnExpressionArgument[_T] | None = None
    default_execution_options: tuple[tuple, dict] = ((), {})
    default_page_size: int = 100

    _default_set = None

    __slots__ = ("db", "_query")

    def __init_subclass__(cls, **kwargs):
        if not (inspect.isabstract(cls) or hasattr(cls, "model")):
            cls.model = cls.__orig_bases__[0].__args__[0]
            if not (cls.model and issubclass(cls.model, ORMModel)):
                raise TypeError(f"Could not resolve model for {cls.__name__}")

    def __init__(
        self,
        db: Database,
        query: ClauseElement | Callable = None,
    ):
        self.db = db
        self._query = query

    @property
    def raw_query(self) -> str:
        return str(self.query)

    def copy(self, query: ClauseElement | Executable | Callable) -> Self:
        return self.__class__(self.db, query)

    def __call__(self, *args, **kwargs) -> Self:
        self._query = self._query(*args, **kwargs)
        return self

    def __getattr__(self, item: str) -> Self:
        self._query = getattr(self._query, item)
        return self

    def __aiter__(self) -> Coroutine[Any, Any, AsyncScalarResult[Any]]:
        return self.db.stream_scalars(self.query)

    def __await__(self) -> Generator[Any, None, Result]:
        return self.execute().__await__()

    @property
    def _insert(self):
        return self.db.insert

    @property
    def _update(self):
        return self.db.update

    @property
    def _delete(self):
        return self.db.delete

    @property
    def _select(self):
        return self.db.select

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
                c.name
                for c in self.model.__table__.primary_key.columns  # type: ignore[attr-defined]
            ],
            "set_": self._get_default_set(),
        }

    def _get_default_select_query(self, *args):
        if len(args) == 0:
            args = (self.model,)
        query = self._select(*args)
        if self.default_order_by is not None:
            query = query.order_by(self.default_order_by)
        return query

    @property
    def query(self) -> ClauseElement | Executable:
        if self._query is None:
            self._query = self._get_default_select_query()
        return self._query

    def filter(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> Self:
        query = self.query
        if args:
            query = query.filter(*args)  # type: ignore[union-attr]
        if kwargs:
            query = query.filter_by(**kwargs)  # type: ignore[union-attr]
        return self.copy(query)

    def where(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> Self:
        return self.filter(*args, **kwargs)

    def select(self, *args: _ColumnsClauseArgument) -> Self:
        query = self._get_default_select_query(*args)
        return self.copy(query)

    def insert(
        self,
        values: Any,
        return_results: bool = True,
        ignore_conflicts: bool = False,
        index_where: _ColumnExpressionArgument[bool] | None = None,
    ) -> Self:
        query = self._insert(self.model).values(values)
        if self.db.supports_returning and return_results:
            query = query.returning(self.model)

        if self.db.supports_on_conflict and ignore_conflicts:
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
        if not self.db.supports_on_conflict:
            raise TypeError("Upsert is not supported")

        query = self._insert(self.model).values(values)
        if self.db.supports_returning and return_results:
            query = query.returning(self.model)
        kwargs.update(**self.on_conflict)
        if set_ is None:
            if isinstance(values, Mapping):
                pk_columns = {c.name for c in self.model.__table__.primary_key.columns}  # type: ignore[attr-defined]
                set_ = {k for k in values if k not in pk_columns}
            else:
                set_ = self.on_conflict.get("set_", self._get_default_set())
        if set_:
            kwargs["set_"] = {k: getattr(query.excluded, k) for k in set_}
        query = query.on_conflict_do_update(**kwargs)
        return self.copy(query)

    def update(self, values: Any, return_results: bool = False) -> Self:
        query = self._update(self.model).values(values)
        if self.db.supports_returning and return_results:
            query = query.returning(self.model)
        return self.copy(query)

    def delete(self, return_results: bool = False) -> Self:
        query = self._delete(self.model)
        if self.db.supports_returning and return_results:
            query = query.returning(self.model)

        return self.copy(query)

    def join(
        self,
        target: _JoinTargetArgument,
        onclause: _OnClauseArgument | None = None,
        *,
        isouter: bool = False,
        full: bool = False,
    ):
        query = self.query.join(target, onclause, isouter=isouter, full=full)  # type: ignore[attr-defined]
        return self.copy(query)

    def joinedload(self, *relationships, **loaded_only) -> Self:
        load_args_options = joinedload(
            *(getattr(self.model, relationship) for relationship in relationships)
        )
        load_kwargs_options = [
            joinedload(getattr(self.model, k)).options(load_only(*v))
            for k, v in loaded_only.items()
        ]
        query = self.query.options(*load_args_options, *load_kwargs_options)
        return self.copy(query)

    def page(self, offset: int | None = None, limit: int | None = None) -> Self:
        query = self.query

        if offset is not None:
            query = query.offset(offset)  # type: ignore[attr-defined]
        if limit is not None:
            query = query.limit(limit)
        return self.copy(query)

    async def paginate(self, offset: int = 0, limit: int = 100) -> Sequence[Model]:
        return await self.page(offset, limit).all()

    async def count(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> int:
        query = self._select(sa.func.count()).select_from(self.model)
        if args:
            query = query.filter(*args)
        if kwargs:
            query = query.filter_by(**kwargs)
        return (await self.execute_query(query)).scalar()

    async def execute_many(
        self, queries: Sequence[Executable], *args, **kwargs
    ) -> Sequence[Result]:
        return await self.db.execute_many(queries, *args, **kwargs)

    async def execute_query(self, query: Executable, *args, **kwargs) -> Result:
        return await self.db.execute(query, *args, **kwargs)

    async def execute(self) -> Result:
        return await self.execute_query(self.query)

    async def mappings(self) -> MappingResult:
        return (await self.execute()).mappings()

    async def scalar(self) -> Any:
        return (await self.execute()).scalar()

    async def scalars(self) -> ScalarResult:
        return (await self.execute()).scalars()

    async def unique(self) -> Any:
        return (await self.scalars()).unique()

    async def all(self, unique: bool = False) -> Sequence[Model]:
        if unique:
            return (await self.scalars()).unique().all()
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
            obj = await self.select().filter(**kwargs).one()
        return obj

    async def create(self, **kwargs) -> Model | None:
        return await self.insert(
            kwargs, ignore_conflicts=True, return_results=True
        ).one_or_none()

    async def remove(self, *args, **kwargs) -> None:
        await self.delete().filter(*args, **kwargs).execute()

    async def delete_one(self, *args, **kwargs) -> Model | None:
        return (
            await self.delete(return_results=True).filter(*args, **kwargs).one_or_none()
        )

    async def delete_many(self, *args, **kwargs) -> Sequence[Model]:
        return await self.delete(return_results=True).filter(*args, **kwargs).all()

    async def list(self, *args, **kwargs) -> Sequence[Model]:
        return await self.select().filter(*args, **kwargs).all()

    async def bulk_create_or_update(
        self,
        values: Sequence[Mapping],
        return_results: bool = False,
        set_: set[str] | None = None,
        **kwargs: Any,
    ) -> Sequence[Model] | Result:
        q = self.upsert(values, return_results=return_results, set_=set_, **kwargs)
        if return_results:
            return await q.all()

        return await q.execute()

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

    async def bulk_update(
        self, values: Sequence[Mapping], on_: set[str], *args
    ) -> None:
        where = [getattr(self.model, field) == bindparam(f"u_{field}") for field in on_]
        values = [
            {key if key not in on_ else f"u_{key}": value for key, value in row.items()}
            for row in values
        ]
        await self.db.execute_from_connection(
            self._update(self.model).where(*args, *where), values
        )

    async def commit(self) -> None:
        await self.db.commit()
