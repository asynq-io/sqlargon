from __future__ import annotations

import inspect
from collections.abc import AsyncIterable, Generator, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

import sqlalchemy as sa
from databases.interfaces import Record
from sqlalchemy import (
    Result,
    Select,
    bindparam,
)
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import DQLDMLClauseElement
from typing_extensions import Self

from .exceptions import EntityNotFoundError
from .orm import Model
from .pagination import BasePage, NumberedPaginationStrategy, PaginationStrategy

if TYPE_CHECKING:
    from sqlalchemy.sql._typing import (
        _ColumnExpressionArgument,
        _ColumnsClauseArgument,
        _JoinTargetArgument,
        _OnClauseArgument,
    )

    from .database import Database

D = TypeVar("D", bound=Any)
_T = TypeVar("_T", bound=Any)


class OnConflict(TypedDict, total=False):
    index_elements: Sequence[str]
    index_where: DQLDMLClauseElement
    set_: set[Any]
    where: DQLDMLClauseElement


Query = Union[ClauseElement, Select]
Values = Union[dict[str, Any], list[dict[str, Any]]]


class SQLAlchemyRepository(Generic[Model]):
    model: type[Model]
    default_order_by: str | _ColumnExpressionArgument | None = None
    default_page_size: int = 100
    paginator: type[PaginationStrategy] = NumberedPaginationStrategy

    _default_set = None

    __slots__ = ("db", "_query")

    def __init_subclass__(cls, **kwargs):
        if not (inspect.isabstract(cls) or hasattr(cls, "model")):
            cls.model = cls.__orig_bases__[0].__args__[0]

    def __init__(
        self,
        db: Database,
        query: Query | None = None,
    ):
        self.db = db
        self._query = query

    @property
    def query(self) -> Query:
        if self._query is None:
            self._query = self._get_default_select_query()
        return self._query

    @property
    def raw_query(self) -> str:
        return str(self.query)

    def copy(self, query: Query) -> Self:
        return self.__class__(self.db, query)

    async def iterate(self) -> AsyncIterable[Model]:
        async for row in self.db.iterate(query=self.query):
            yield self.model(**row)

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

    def _get_default_select_query(self, *args) -> Select:
        if len(args) == 0:
            args = (self.model,)
        query = self._select(*args)
        if self.default_order_by is not None:
            query = query.order_by(self.default_order_by)
        return query

    def filter(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> Self:
        query = cast(Select, self.query)
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
        values: Values | None = None,
        *,
        return_results: bool = True,
        ignore_conflicts: bool = False,
        index_elements: Sequence[str] | None = None,
        index_where: DQLDMLClauseElement | None = None,
    ) -> Self:
        query = self._insert(self.model)
        if values:
            query = query.values(values)
        if index_elements is None:
            index_elements = self.on_conflict.get("index_elements")
        if index_where is None:
            index_where = self.on_conflict.get("index_where")
        if ignore_conflicts:
            query = query.on_conflict_do_nothing(
                index_elements=index_elements, index_where=index_where
            )
        if return_results:
            query = query.returning(self.model)

        return self.copy(query)

    def upsert(
        self,
        values: Values | None = None,
        *,
        return_results: bool = False,
        set_: set[str] | None = None,
        **kwargs: Any,
    ) -> Self:
        query = self._insert(self.model)

        set_fields = set_ or self.on_conflict.get("set_", {})
        set_dict = {k: getattr(query.excluded, k) for k in set_fields}
        query = query.on_conflict_do_update(set_=set_dict, **kwargs)
        if return_results:
            query = query.returning(self.model)
        if values:
            query = query.values(values)
        return self.copy(query)

    def update(
        self, values: Values | None = None, *, return_results: bool = False
    ) -> Self:
        query = self._update(self.model)
        if values:
            query = query.values(values)
        if return_results:
            query = query.returning(self.model)
        return self.copy(query)

    def delete(self, *, return_results: bool = False) -> Self:
        query = self._delete(self.model)
        if return_results:
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

    def joinedload(self, *keys, **kwargs) -> Self:
        assert isinstance(self.query, Select)
        query = self.query.options(joinedload(*keys, **kwargs))
        return self.copy(query)

    async def count(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> int:
        query = self._select(sa.func.count()).select_from(self.model)
        if args:
            query = query.filter(*args)
        if kwargs:
            query = query.filter_by(**kwargs)
        return await self.db.fetch_val(query=query)

    async def execute_many(self, values: list[dict[str, Any]]):
        return await self.db.execute_many(query=self.query, values=values)

    async def execute(self, values: dict[str, Any] | None = None) -> Result:
        return await self.db.execute(query=self.query, values=values)

    async def all(self) -> Sequence[Record]:
        return await self.db.fetch_all(self.query)

    async def one_or_none(self) -> Record | None:
        return await self.db.fetch_one(query=self.query)

    async def one(self) -> Record:
        obj = await self.one_or_none()
        if obj is None:
            raise EntityNotFoundError()
        return obj

    async def get(self, *args, **kwargs) -> Model | None:
        obj = await self.filter(*args, **kwargs).one_or_none()
        if obj is not None:
            return cast(Model, obj)
        return None

    async def create_or_update(self, **kwargs) -> Model:
        obj = await self.upsert(kwargs, return_results=True).one()
        return cast(Model, obj)

    async def get_or_create(
        self, defaults: dict[str, Any] | None = None, **kwargs
    ) -> Model:
        defaults = defaults or {}
        defaults.update(kwargs)
        obj = await self.insert(defaults, ignore_conflicts=True).one_or_none()
        if obj is None:
            obj = await self.select().filter(**kwargs).one()
        return cast(Model, obj)

    async def create(self, **kwargs) -> Model | None:
        obj = await self.insert(
            kwargs, ignore_conflicts=True, return_results=True
        ).one_or_none()
        if obj is not None:
            return cast(Model, obj)
        return None

    async def remove(self, *args, **kwargs) -> Model | None:
        obj = await self.delete(return_results=True).filter(*args, **kwargs).execute()
        if obj is not None:
            return cast(Model, obj)
        return None

    async def delete_one(self, *args, **kwargs) -> None:
        await self.delete().filter(*args, **kwargs).one_or_none()

    async def delete_many(self, *args, **kwargs) -> None:
        await self.delete().filter(*args, **kwargs).execute()

    async def bulk_create_or_update(
        self,
        values: list[dict[str, Any]],
        *,
        return_results: bool = False,
        set_: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        await self.upsert(
            return_results=return_results, set_=set_, **kwargs
        ).execute_many(values)

    async def bulk_create(
        self,
        values: list[dict[str, Any]],
        ignore_conflicts: bool = True,
        return_results: bool = False,
    ) -> None:
        await self.insert(
            ignore_conflicts=ignore_conflicts, return_results=return_results
        ).execute_many(values)

    async def bulk_update(
        self, values: list[dict[str, Any]], on_: set[str], *where
    ) -> None:
        bind_params = [getattr(self.model, p) == bindparam(f"u{p}") for p in on_]
        for row in values:
            sub_row = {f"u{p}": row[p] for p in on_}
            row.update(sub_row)
        query = self._update(self.model).where(*where, *bind_params)
        await self.db.execute_many(query, values=values)

    async def get_page(
        self, page: Any = None, page_size: int = 100, as_model: bool = True, **kwargs
    ) -> BasePage[Model]:
        assert isinstance(self.query, Select)
        paginator = self.paginator(self.db, self.query)
        return await paginator.paginate(
            page=page, page_size=page_size, as_model=as_model, **kwargs
        )

    async def list(self, *args, **kwargs) -> Sequence[Model]:
        result = await self.select().filter(*args, **kwargs).all()
        return cast(Sequence[Model], result)
