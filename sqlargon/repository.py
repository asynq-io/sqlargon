from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, overload

from sqlalchemy import (
    Executable,
    MappingResult,
    Result,
    Row,
    ScalarResult,
    bindparam,
)
from sqlalchemy.orm import QueryableAttribute, selectinload

from .orm import Base, Model
from .registry import get_default_database
from .routing import RoutingContext, RoutingOptions
from .typing import (
    MultipleValues,
    OnConflict,
    OnConflictOptions,
    Params,
    SingleValue,
    Values,
    WithForUpdate,
)

if TYPE_CHECKING:
    from collections.abc import (
        AsyncGenerator,
        AsyncIterator,
        Callable,
        Generator,
        Sequence,
    )

    from sqlalchemy.ext.asyncio.session import AsyncSession
    from sqlalchemy.sql._typing import (
        _ColumnExpressionArgument,
        _JoinTargetArgument,
        _OnClauseArgument,
    )
    from sqlalchemy.sql.selectable import TypedReturnsRows
    from typing_extensions import Self, Unpack

    from sqlargon.database import Database

    from .cluster import AnyDatabase
    from .query_builder import QueryBuilder


class SQLAlchemyRepository(Generic[Model]):
    """Repository over a model, bound to a database or cluster.

    The database resolves to a :meth:`using` override if given, otherwise to
    the process-wide default built from ``DATABASE_*`` settings -- so in the
    common single-database case no binding is needed at all. Repositories
    accessed through a unit of work are bound to the unit's database.

    ``__init__`` deliberately takes no arguments so subclasses work directly
    as FastAPI dependencies (``Depends(UserRepository)``) without leaking
    routing knobs as query parameters.
    """

    __slots__ = ("_query", "routing")

    database: ClassVar[Database | None] = None
    model: type[Model]
    default_order_by: str | _ColumnExpressionArgument | None = None

    def __init__(self) -> None:
        self._query: Any = None
        self.routing = RoutingOptions()

    def __init_subclass__(
        cls,
        *,
        abstract: bool = False,
        model: type[Model] | None = None,
        **kwargs: Any,
    ) -> None:
        if model is not None:
            cls.model = model
        elif not abstract:
            if not hasattr(cls, "model"):
                cls.model = cls.__orig_bases__[0].__args__[0]  # type: ignore[attr-defined]
            if not issubclass(cls.model, Base):
                msg = f"Could not resolve model for {cls.__name__}"
                raise TypeError(msg)
        super().__init_subclass__(**kwargs)

    @property
    def db(self) -> AnyDatabase:
        if self.routing.db is not None:
            return self.routing.db
        if self.database is not None:
            return self.database
        return get_default_database()

    @property
    def qb(self) -> QueryBuilder:
        return self.db.query_builder

    @property
    def query(self) -> Any:
        if self._query is None:
            query = self.qb.select(self.model)
            if self.default_order_by is not None:
                query = query.order_by(self.default_order_by)
            self._query = query
        return self._query

    def __getattr__(self, attribute: str) -> Self:
        self._query = getattr(self.query, attribute)
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> Self:
        self._query = self.query(*args, **kwargs)
        return self

    def __await__(self) -> Generator[Any, None, Result]:
        return self.execute().__await__()

    @classmethod
    def _get_default_index_elements(cls) -> set[str]:
        return {
            c.name
            for c in cls.model.__table__.primary_key.columns  # type: ignore[attr-defined]
        }

    @classmethod
    def _get_default_set(cls) -> set[str]:
        return {
            c.name
            for c in cls.model.__table__.columns  # type: ignore[attr-defined]
            if c.name not in cls._get_default_index_elements()
        }

    @property
    def on_conflict(self) -> OnConflictOptions:
        return {
            "index_elements": self._get_default_index_elements(),
            "set_": self._get_default_set(),
        }

    def use_query(self, query: Any) -> Self:
        self._query = query
        return self

    def copy(self, query: Any) -> Self:
        clone = self.__class__().use_query(query)
        clone.routing = self.routing
        return clone

    def using(
        self,
        hint: str | None = None,
        *,
        db: AnyDatabase | None = None,
        read_only: bool = False,
        shard_key: Any | None = None,
    ) -> Self:
        """Return a copy of this repository with a routing preference baked in.

        The returned repository routes its statements to the given database,
        replica or shard, unless a transaction already pins another one::

            await repo.using("replica_0").all()
            await repo.using(read_only=True).count()
            await repo.using(shard_key=tenant_id).create(**values)
            await repo.using(db=other_database).all()
        """
        clone = self.copy(self._query)
        clone.routing = self.routing.merge(
            hint, db=db, read_only=read_only, shard_key=shard_key
        )
        return clone

    def insert(
        self,
        values: Values,
        *,
        return_results: bool = False,
        ignore_conflicts: bool = False,
        **options: Unpack[OnConflictOptions],
    ) -> Self:
        on_conflict = None
        if ignore_conflicts:
            on_conflict = OnConflict(do="ignore", options=self.on_conflict | options)
        query = self.qb.insert(
            self.model, values, return_results=return_results, on_conflict=on_conflict
        )
        return self.copy(query)

    def upsert(
        self,
        values: Values,
        *,
        return_results: bool = False,
        **options: Unpack[OnConflictOptions],
    ) -> Self:
        query = self.qb.insert(
            self.model,
            values,
            return_results=return_results,
            on_conflict=OnConflict(do="update", options=self.on_conflict | options),
        )
        return self.copy(query)

    def select(
        self,
        *args: Any,
        with_for_update: bool | WithForUpdate | None = None,
        options: tuple[Any, ...] | None = None,
    ) -> Self:
        args = args or (self.model,)
        query = self.qb.select(*args, with_for_update=with_for_update, options=options)
        return self.copy(query)

    def update(self, values: Values, *, return_results: bool = False) -> Self:
        query = self.qb.update(self.model, values, return_results=return_results)
        return self.copy(query)

    def delete(self, *, return_results: bool = False) -> Self:
        query = self.qb.delete(self.model, return_results=return_results)
        return self.copy(query)

    def where(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> Self:
        query = self.qb.filter(self.query, *args, **kwargs)
        return self.copy(query)

    def filter(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> Self:
        return self.where(*args, **kwargs)

    def join(
        self,
        target: _JoinTargetArgument,
        onclause: _OnClauseArgument | None = None,
        *,
        isouter: bool = False,
        full: bool = False,
    ) -> Self:
        query = self.query.join(target, onclause, isouter=isouter, full=full)  # type: ignore[attr-defined]
        return self.copy(query)

    def load(self, *keys: QueryableAttribute) -> Self:
        query = self.query.options(selectinload(*keys))
        return self.copy(query)

    def routing_context(
        self, statement: Any = None, *, read_only: bool = False
    ) -> RoutingContext:
        return RoutingContext.create(
            statement=statement,
            model=getattr(type(self), "model", None),
            read_only=read_only or self.routing.read_only,
            hint=self.routing.hint,
            shard_key=self.routing.shard_key,
        )

    @asynccontextmanager
    async def session(
        self, statement: Any = None, *, read_only: bool = False
    ) -> AsyncGenerator[AsyncSession]:
        context = self.routing_context(statement, read_only=read_only)
        async with self.db.session_context(context) as session:
            yield session

    async def execute_query(
        self,
        query: Executable | TypedReturnsRows,
        params: Params | None = None,
        *,
        read_only: bool = False,
        **kwargs: Any,
    ) -> Result:
        async with self.session(query, read_only=read_only) as session:
            return await session.execute(query, params, **kwargs)

    async def execute(
        self, params: Params | None = None, *, read_only: bool = False, **kwargs: Any
    ) -> Result:
        return await self.execute_query(
            self.query, params, read_only=read_only, **kwargs
        )

    async def execute_many(
        self, *queries: Executable | TypedReturnsRows, **kwargs: Any
    ) -> None:
        async with self.session() as session:
            for query in queries:
                await session.execute(query, **kwargs)

    async def stream_query(
        self,
        query: Executable | TypedReturnsRows,
        params: Params | None = None,
        *,
        read_only: bool = False,
        **kwargs: Any,
    ) -> AsyncIterator[Row[Any]]:
        async with self.session(query, read_only=read_only) as session:
            rows = await session.stream(query, params, **kwargs)
            async for row in rows:
                yield row

    def stream(
        self, params: Params | None = None, *, read_only: bool = False, **kwargs: Any
    ) -> AsyncIterator[Row[Any]]:
        return self.stream_query(self.query, params, read_only=read_only, **kwargs)

    async def mappings(self) -> MappingResult:
        return (await self.execute()).mappings()

    async def scalar(self) -> Any:
        return (await self.execute()).scalar()

    async def scalars(self) -> ScalarResult[Model]:
        return (await self.execute()).scalars()

    async def unique(
        self, strategy: Callable[[Any], Any] | None = None
    ) -> ScalarResult[Model]:
        return (await self.scalars()).unique(strategy)

    async def all(
        self, *, unique: bool | Callable[[Any], Any] = False
    ) -> Sequence[Model]:
        if not unique:
            return (await self.scalars()).all()
        strategy = unique if callable(unique) else None
        return (await self.unique(strategy)).all()

    async def one(self) -> Model:
        return (await self.scalars()).one()

    async def one_or_none(self) -> Model | None:
        return (await self.scalars()).one_or_none()

    async def first(self) -> Model | None:
        return (await self.scalars()).first()

    async def get(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Model | None:
        return await self.filter(*args, **kwargs).one_or_none()

    async def create_or_update(self, **kwargs: Any) -> Model:
        return await self.upsert(kwargs, return_results=True).one()

    async def get_or_create(
        self, defaults: SingleValue | None = None, **kwargs: Any
    ) -> Model:
        async with self.session():
            values = {**(defaults or {}), **kwargs}
            obj = await self.insert(
                values, return_results=True, ignore_conflicts=True
            ).one_or_none()
            if obj is None:
                obj = await self.select().filter(**kwargs).one()
            return obj

    async def create(self, **kwargs: Any) -> Model | None:
        return await self.insert(
            kwargs, return_results=True, ignore_conflicts=True
        ).one_or_none()

    async def remove(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        await self.delete().filter(*args, **kwargs).execute()

    async def delete_one(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Model | None:
        return (
            await self.delete(return_results=True).filter(*args, **kwargs).one_or_none()
        )

    async def update_one(
        self, values: SingleValue, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Model | None:
        return (
            await self.update(values, return_results=True)
            .filter(*args, **kwargs)
            .one_or_none()
        )

    async def update_many(
        self, values: SingleValue, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        await self.update(values).filter(*args, **kwargs).execute()

    async def delete_many(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Sequence[Model]:
        return await self.delete(return_results=True).filter(*args, **kwargs).all()

    async def list(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Sequence[Model]:
        return await self.select().filter(*args, **kwargs).all()

    @overload
    async def bulk_create_or_update(
        self,
        values: MultipleValues,
        *,
        return_results: Literal[False] = ...,
        **options: Unpack[OnConflictOptions],
    ) -> Result: ...

    @overload
    async def bulk_create_or_update(
        self,
        values: MultipleValues,
        *,
        return_results: Literal[True],
        **options: Unpack[OnConflictOptions],
    ) -> Sequence[Model]: ...

    async def bulk_create_or_update(
        self,
        values: MultipleValues,
        *,
        return_results: bool = False,
        **options: Unpack[OnConflictOptions],
    ) -> Sequence[Model] | Result:
        q = self.upsert(values, return_results=return_results, **options)
        if return_results:
            return await q.all()

        return await q.execute()

    @overload
    async def bulk_create(
        self,
        values: MultipleValues,
        *,
        ignore_conflicts: bool = ...,
        return_results: Literal[False] = ...,
    ) -> None: ...

    @overload
    async def bulk_create(
        self,
        values: MultipleValues,
        *,
        ignore_conflicts: bool = ...,
        return_results: Literal[True],
    ) -> Sequence[Model]: ...

    async def bulk_create(
        self,
        values: MultipleValues,
        *,
        ignore_conflicts: bool = True,
        return_results: bool = False,
        **options: Unpack[OnConflictOptions],
    ) -> Sequence[Model] | None:
        q = self.insert(
            values,
            ignore_conflicts=ignore_conflicts,
            return_results=return_results,
            **options,
        )
        if return_results:
            return await q.all()
        await q.execute()
        return None

    @overload
    async def bulk_update(
        self,
        values: MultipleValues,
        on_: set[str],
        *args: Any,
        return_results: Literal[False] = ...,
    ) -> None: ...

    @overload
    async def bulk_update(
        self,
        values: MultipleValues,
        on_: set[str],
        *args: Any,
        return_results: Literal[True],
    ) -> Sequence[Model]: ...

    async def bulk_update(
        self,
        values: MultipleValues,
        on_: set[str],
        *args: Any,
        return_results: bool = False,
    ) -> Sequence[Model] | None:
        where = [getattr(self.model, field) == bindparam(f"u_{field}") for field in on_]
        values = [
            {key if key not in on_ else f"u_{key}": value for key, value in row.items()}
            for row in values
        ]
        query = self.qb.update(self.model, return_results=return_results).where(
            *args, *where
        )
        async with self.session() as session:
            connection = await session.connection()
            results = await connection.execute(query, values)
            if return_results:
                return results.scalars().all()
        return None

    async def count(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> int:
        query = self.qb.count(self.model, *args, **kwargs)
        return (await self.execute_query(query)).scalar()  # type: ignore[return-value]

    @asynccontextmanager
    async def get_chunk_for_update(
        self,
        values: SingleValue | None,
        limit: int = 100,
        on_: str = "id",
        order_by: Any = None,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncGenerator[Sequence[Model]]:
        pk = getattr(self.model, on_)
        order_by = order_by or pk
        async with self.session():
            results = (
                await self.select(with_for_update={"skip_locked": True})
                .filter(*args, **kwargs)
                .order_by(order_by)
                .limit(limit)
                .all()
            )

            yield results
            if values:
                await self.update_many(
                    values, pk.in_([getattr(r, on_) for r in results])
                )
            else:
                await self.remove(pk.in_([getattr(r, on_) for r in results]))
