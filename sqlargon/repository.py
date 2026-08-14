from __future__ import annotations

from collections.abc import Mapping as MappingABC
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, overload

from sqlalchemy import (
    Delete,
    Executable,
    MappingResult,
    Result,
    Row,
    ScalarResult,
    SQLColumnExpression,
    Text,
    and_,
    bindparam,
    cast,
    false,
    or_,
)
from sqlalchemy.orm import QueryableAttribute, selectinload

from .mixins import SoftDeleteMixin, VersionedMixin
from .orm import Base, Model, SoftDeleteModel, VersionedModel
from .query_builder import Option, UnsupportedOption
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
    default_order_by: ClassVar[str | SQLColumnExpression[Any] | None] = None

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
            # read off the class: a bare mapped column is a descriptor, and
            # resolving it against a repository instance would fall through
            # to __getattr__
            order_by = type(self).default_order_by
            if order_by is not None:
                query = query.order_by(order_by)
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
        read_only: bool | None = None,
        shard_key: Any | None = None,
    ) -> Self:
        """Return a copy of this repository with a routing preference baked in.

        The returned repository routes its statements to the given database,
        replica or shard, unless a transaction already pins another one::

            await repo.using("replica_0").all()
            await repo.using(read_only=True).count()
            await repo.using(shard_key=tenant_id).create(**values)
            await repo.using(db=other_database).all()

        ``read_only=False`` explicitly forces the primary even when the
        repository inherited ``read_only=True``; ``None`` keeps it.
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
        self, statement: Any = None, *, read_only: bool | None = None
    ) -> RoutingContext:
        return self.routing.context(
            statement, getattr(type(self), "model", None), read_only=read_only
        )

    @asynccontextmanager
    async def session(
        self, statement: Any = None, *, read_only: bool | None = None
    ) -> AsyncGenerator[AsyncSession]:
        context = self.routing_context(statement, read_only=read_only)
        async with self.db.session_context(context) as session:
            yield session

    async def execute_query(
        self,
        query: Executable | TypedReturnsRows,
        params: Params | None = None,
        *,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> Result:
        async with self.session(query, read_only=read_only) as session:
            return await session.execute(query, params, **kwargs)

    async def execute(
        self,
        params: Params | None = None,
        *,
        read_only: bool | None = None,
        **kwargs: Any,
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
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[Row[Any]]:
        async with self.session(query, read_only=read_only) as session:
            rows = await session.stream(query, params, **kwargs)
            async for row in rows:
                yield row

    def stream(
        self,
        params: Params | None = None,
        *,
        read_only: bool | None = None,
        **kwargs: Any,
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

    @classmethod
    def _column(cls, name: str) -> Any:
        return cls.model.__table__.columns[name]  # type: ignore[attr-defined]

    @classmethod
    def _identity_elements(
        cls, options: OnConflictOptions | None = None
    ) -> tuple[str, ...]:
        elements = (options or {}).get("index_elements")
        return tuple(elements or cls._get_default_index_elements())

    @staticmethod
    def _identity(values: SingleValue, elements: tuple[str, ...]) -> tuple[Any, ...]:
        return tuple(values[name] for name in elements)

    def _identified(
        self, values: SingleValue, elements: tuple[str, ...]
    ) -> dict[str, Any]:
        row = dict(values)
        for name in elements:
            if row.get(name) is not None:
                continue
            default = self._column(name).default
            if default is None or not (default.is_scalar or default.is_callable):
                msg = (
                    f"{self.model.__name__}.{name} is filled by the server, so the "
                    f"{self.db.dialect} dialect cannot return the written rows "
                    "without a RETURNING clause"
                )
                raise UnsupportedOption(msg)
            row[name] = default.arg(None) if default.is_callable else default.arg
        return row

    def _identity_filter(
        self, rows: Sequence[SingleValue], elements: tuple[str, ...]
    ) -> Any:
        if not rows:
            return false()
        if len(elements) == 1:
            name = elements[0]
            return self._column(name).in_([row[name] for row in rows])
        return or_(
            *(
                and_(*(self._column(name) == row[name] for name in elements))
                for row in rows
            )
        )

    async def _identities(
        self, elements: tuple[str, ...], where: Any = None
    ) -> list[tuple[Any, ...]]:
        query = self.qb.select(*(self._column(name) for name in elements))
        if where is not None:
            query = query.where(where)
        return [tuple(row) for row in (await self.execute_query(query)).all()]

    async def _fetch(self, where: Any = None) -> ScalarResult[Model]:
        query = self.qb.select(self.model)
        if where is not None:
            query = query.where(where)
        return (await self.execute_query(query)).scalars()

    def _insert_statement(
        self,
        values: Values,
        do: Literal["ignore", "update"] | None,
        options: OnConflictOptions,
        *,
        return_results: bool,
    ) -> Self:
        if do == "update":
            return self.upsert(values, return_results=return_results, **options)
        return self.insert(
            values,
            return_results=return_results,
            ignore_conflicts=do == "ignore",
            **options,
        )

    async def _insert_returning(
        self,
        values: MultipleValues,
        *,
        do: Literal["ignore", "update"] | None = None,
        **options: Unpack[OnConflictOptions],
    ) -> ScalarResult[Model]:
        if self.qb.supports(Option.RETURNING):
            statement = self._insert_statement(values, do, options, return_results=True)
            return await statement.scalars()

        elements = self._identity_elements(self.on_conflict | options)
        rows = [self._identified(row, elements) for row in values]
        statement = self._insert_statement(rows, do, options, return_results=False)
        async with self.session(statement.query):
            written = rows
            if do == "ignore":
                identities = await self._identities(
                    elements, self._identity_filter(rows, elements)
                )
                stored = set(identities)
                written = [
                    row for row in rows if self._identity(row, elements) not in stored
                ]
            await statement.execute()
            return await self._fetch(self._identity_filter(written, elements))

    async def _write_returning(self, statement: Self) -> ScalarResult[Model]:
        query = statement.query
        async with self.session(query):
            if isinstance(query, Delete):
                rows = await self._fetch(query.whereclause)
                await statement.execute()
                return rows
            elements = self._identity_elements()
            identities = await self._identities(elements, query.whereclause)
            await statement.execute()
            written = [
                dict(zip(elements, identity, strict=True)) for identity in identities
            ]
            return await self._fetch(self._identity_filter(written, elements))

    async def _update_returning(
        self, values: Values, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> ScalarResult[Model]:
        if self.qb.supports(Option.RETURNING):
            statement = self.update(values, return_results=True)
            return await statement.filter(*args, **kwargs).scalars()
        return await self._write_returning(self.update(values).filter(*args, **kwargs))

    async def _delete_returning(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> ScalarResult[Model]:
        if self.qb.supports(Option.RETURNING):
            statement = self.delete(return_results=True)
            return await statement.filter(*args, **kwargs).scalars()
        return await self._write_returning(self.delete().filter(*args, **kwargs))

    async def create_or_update(self, **kwargs: Any) -> Model:
        result = await self._insert_returning([kwargs], do="update")
        return result.one()

    async def get_or_create(
        self, defaults: SingleValue | None = None, **kwargs: Any
    ) -> Model:
        async with self.session():
            values = {**(defaults or {}), **kwargs}
            result = await self._insert_returning([values], do="ignore")
            created = result.one_or_none()
            if created is not None:
                return created
            return await self.select().filter(**kwargs).one()

    async def create(self, **kwargs: Any) -> Model | None:
        result = await self._insert_returning([kwargs], do="ignore")
        return result.one_or_none()

    async def remove(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        await self.delete().filter(*args, **kwargs).execute()

    async def delete_one(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Model | None:
        result = await self._delete_returning(*args, **kwargs)
        return result.one_or_none()

    async def update_one(
        self, values: SingleValue, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Model | None:
        result = await self._update_returning(values, *args, **kwargs)
        return result.one_or_none()

    async def update_many(
        self, values: SingleValue, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Sequence[Model]:
        result = await self._update_returning(values, *args, **kwargs)
        return result.all()

    async def delete_many(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        await self.delete(return_results=False).filter(*args, **kwargs).execute()

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
        if return_results:
            result = await self._insert_returning(values, do="update", **options)
            return result.all()
        return await self.upsert(values, **options).execute()

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
        if return_results:
            result = await self._insert_returning(
                values, do="ignore" if ignore_conflicts else None, **options
            )
            return result.all()
        await self.insert(
            values, ignore_conflicts=ignore_conflicts, **options
        ).execute()
        return None

    async def create_many(
        self,
        items: MultipleValues,
        *,
        ignore_conflicts: bool = False,
        **kwargs: Any,
    ) -> Sequence[Model]:
        return await self.bulk_create(
            items, return_results=True, ignore_conflicts=ignore_conflicts, **kwargs
        )

    async def bulk_update(
        self,
        values: MultipleValues,
        *args: Any,
        on_: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Update many rows in a single executemany statement.

        An executemany cannot return rows; use :meth:`update_many` when the
        updated models are needed.
        """
        on_ = on_ or self._get_default_index_elements()
        where = [getattr(self.model, field) == bindparam(f"u_{field}") for field in on_]
        values = [
            {key if key not in on_ else f"u_{key}": value for key, value in row.items()}
            for row in values
        ]
        query = self.qb.update(self.model).where(*args, *where).filter_by(**kwargs)
        async with self.session() as session:
            connection = await session.connection()
            await connection.execute(query, values)

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


class DeletedRowExistsError(RuntimeError):
    """A tombstoned row holds the unique key a new row was to be created with."""


class SoftDeleteRepository(SQLAlchemyRepository[SoftDeleteModel], abstract=True):
    """Repository that tombstones rows instead of deleting them.

    Every statement is scoped to live rows: selects and :meth:`count` skip
    tombstoned rows, :meth:`update` refuses to touch them, and :meth:`delete`
    is rewritten into an update raising the flag -- so ``remove``,
    ``delete_one`` and ``delete_many`` all soft delete::

        class User(UUIDModelMixin, SoftDeleteBase): ...


        class UserRepository(SoftDeleteRepository[User]): ...


        users = UserRepository()

        await users.remove(User.id == user_id)  # UPDATE ... SET tombstone = true
        await users.list()  # the row is gone from reads
        await users.restore(User.id == user_id)  # and back again

    The flag belongs to :meth:`delete` and :meth:`restore` alone: it is left
    out of the default ``ON CONFLICT DO UPDATE`` set, so an upsert cannot
    silently resurrect a deleted row. Reach past the scope with
    :meth:`with_deleted`, :meth:`only_deleted` and :meth:`hard_delete`.

    The model type variable is bound to
    :class:`~sqlargon.mixins.SoftDeleteBase`, so a type checker rejects a
    model that cannot be soft deleted. At runtime the looser
    :class:`~sqlargon.mixins.SoftDeleteMixin` is enough; anything else raises
    ``TypeError`` on subclassing.
    """

    __slots__ = ("deleted_only", "include_deleted")

    def __init__(self) -> None:
        super().__init__()
        self.include_deleted = False
        self.deleted_only = False

    def __init_subclass__(cls, *, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(abstract=abstract, **kwargs)
        if not abstract and not issubclass(cls.model, SoftDeleteMixin):
            msg = (
                f"{cls.model.__name__} must inherit from SoftDeleteMixin "
                f"to be used with {cls.__name__}"
            )
            raise TypeError(msg)

    @classmethod
    def _get_default_set(cls) -> set[str]:
        return super()._get_default_set() - {"tombstone"}

    @property
    def _not_deleted(self) -> _ColumnExpressionArgument[bool]:
        return self.model.not_deleted

    @property
    def _is_deleted(self) -> _ColumnExpressionArgument[bool]:
        return self.model.is_deleted

    @property
    def _scope(self) -> _ColumnExpressionArgument[bool] | None:
        """The predicate every statement is narrowed to, if any."""
        if self.deleted_only:
            return self._is_deleted
        if self.include_deleted:
            return None
        return self._not_deleted

    def _scoped(self, query: Any) -> Any:
        """Narrow ``query`` to the rows this repository is scoped to."""
        scope = self._scope
        if scope is None:
            return query
        return query.where(scope)

    def copy(self, query: Any) -> Self:
        clone = super().copy(query)
        clone.include_deleted = self.include_deleted
        clone.deleted_only = self.deleted_only
        return clone

    @property
    def query(self) -> Any:
        if self._query is None:
            self.use_query(self._scoped(super().query))
        return self._query

    def select(
        self,
        *args: Any,
        with_for_update: bool | WithForUpdate | None = None,
        options: tuple[Any, ...] | None = None,
    ) -> Self:
        clone = super().select(*args, with_for_update=with_for_update, options=options)
        return clone.use_query(self._scoped(clone.query))

    def update(self, values: Values, *, return_results: bool = False) -> Self:
        clone = super().update(values, return_results=return_results)
        return clone.use_query(self._scoped(clone.query))

    def delete(self, *, return_results: bool = False) -> Self:
        """Raise the tombstone on the matched rows instead of removing them."""
        return self.update({"tombstone": True}, return_results=return_results)

    async def count(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> int:
        scope = self._scope
        if scope is not None:
            args = (*args, scope)
        return await super().count(*args, **kwargs)

    async def bulk_update(
        self,
        values: MultipleValues,
        *args: Any,
        on_: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        scope = self._scope
        if scope is not None:
            args = (*args, scope)
        await super().bulk_update(values, *args, on_=on_, **kwargs)

    async def get_or_create(
        self, defaults: SingleValue | None = None, **kwargs: Any
    ) -> SoftDeleteModel:
        """Get the live row matching ``kwargs`` or create it.

        Raises :class:`DeletedRowExistsError` when the row exists but is
        tombstoned: creating it would violate the unique key, and returning it
        would resurrect a deleted row behind the caller's back.
        """
        async with self.session():
            values = {**(defaults or {}), **kwargs}
            result = await self._insert_returning([values], do="ignore")
            created = result.one_or_none()
            if created is not None:
                return created
            obj = await self.select().filter(**kwargs).one_or_none()
            if obj is None:
                msg = (
                    f"A deleted {self.model.__name__} row already matches "
                    f"{kwargs}; restore or hard delete it first"
                )
                raise DeletedRowExistsError(msg)
            return obj

    def with_deleted(self) -> Self:
        """Return a copy whose statements cover tombstoned rows as well."""
        clone = self.copy(self._query)
        clone.include_deleted = True
        clone.deleted_only = False
        return clone

    def only_deleted(self) -> Self:
        """Return a copy scoped to tombstoned rows.

        The scope holds for every statement the copy builds, so
        ``only_deleted().count()`` counts the trash and
        ``only_deleted().hard_delete()`` empties it.
        """
        clone = self.copy(self._query)
        clone.include_deleted = True
        clone.deleted_only = True
        return clone

    async def restore(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Sequence[SoftDeleteModel]:
        """Clear the tombstone on the matched rows and return them."""
        return await self.only_deleted().update_many(
            {"tombstone": False}, *args, **kwargs
        )

    async def hard_delete(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        """Physically delete the matched rows, bypassing the tombstone."""
        if self.deleted_only:
            args = (*args, self._is_deleted)
        await super().delete().filter(*args, **kwargs).execute()


class ConcurrentModificationError(RuntimeError):
    """An update or delete matched zero rows — the version was stale."""


class VersionedRepository(SQLAlchemyRepository[VersionedModel], abstract=True):
    """Repository that auto-increments the version column on every update
    and offers optimistic concurrency checks via
    :meth:`update_if_match` / :meth:`delete_if_match`.

    Works with both :class:`~sqlargon.mixins.UUIDVersionedMixin` (UUID
    column, client-managed) and
    :class:`~sqlargon.mixins.XminVersionedMixin` (PostgreSQL ``xmin``,
    server-managed) — the strategy is read from the SQLAlchemy mapper at
    class creation time::

        class User(UUIDModelMixin, VersionedBase): ...


        class UserRepository(VersionedRepository[User]): ...


        users = UserRepository()

        user = await users.get(id=user_id)
        updated = await users.update_if_match(
            {"name": "jane"}, User.id == user_id, expected_version=user.version_id
        )
        if updated is None:
            # someone else modified the row first

    The version column is left out of the default ``ON CONFLICT DO
    UPDATE`` set, so an upsert cannot silently clobber a version. Regular
    ``update_one`` / ``update_many`` / ``bulk_update`` all auto-increment
    the version but do **not** check it — use ``update_if_match`` /
    ``delete_if_match`` for the optimistic guard, or add a manual
    ``Model.version_id == expected`` filter to any method.

    The model type variable is bound to
    :class:`~sqlargon.orm.VersionedBase`, so a type checker rejects a
    model that cannot be versioned. At runtime the looser
    :class:`~sqlargon.mixins.VersionedMixin` is enough; anything else
    raises ``TypeError`` on subclassing.
    """

    def __init_subclass__(cls, *, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(abstract=abstract, **kwargs)
        if abstract:
            return
        if not issubclass(cls.model, VersionedMixin):
            msg = (
                f"{cls.model.__name__} must inherit from VersionedMixin "
                f"(UUIDVersionedMixin or XminVersionedMixin) to be used "
                f"with {cls.__name__}"
            )
            raise TypeError(msg)
        if cls._version_col() is None:
            msg = (
                f"{cls.model.__name__} maps no version column: its "
                f"__mapper_args__ carry no version_id_col, so "
                f"{cls.__name__} could not tell a stale row from a fresh one"
            )
            raise TypeError(msg)

    @classmethod
    def _version_col(cls) -> Any:
        """The version column from the mapper, or ``None``."""
        return getattr(cls.model.__mapper__, "version_id_col", None)

    @classmethod
    def _version_generator(cls) -> Any:
        """The version generator: ``False`` (server-side), ``True``
        (integer increment), or a callable."""
        return getattr(cls.model.__mapper__, "version_id_generator", True)

    @classmethod
    def _is_server_versioned(cls) -> bool:
        return cls._version_generator() is False

    @classmethod
    def _get_default_set(cls) -> set[str]:
        col = cls._version_col()
        excluded = {col.name} if col is not None else set()
        return super()._get_default_set() - excluded

    def _with_version_increment(self, values: Values) -> Values:
        """Return ``values`` with the version column bumped."""
        col = self._version_col()
        generator = self._version_generator()
        if col is None or generator is False:
            return values
        name = col.name
        if callable(generator):
            if isinstance(values, MappingABC):
                return {**values, name: generator(None)}
            return [{**row, name: generator(None)} for row in values]
        # generator is True — integer increment via SQL expression
        if isinstance(values, MappingABC):
            return {**values, name: col + 1}
        # integer increment is not supported for executemany
        return values

    def _version_filter(self, expected: Any) -> Any:
        """The guard matching ``expected`` against the version column.

        A server managed version is PostgreSQL's ``xmin``, an ``xid`` no
        driver binds natively: asyncpg decodes it into an ``int`` while a
        version round tripped through a client comes back as a ``str``, and
        ``xid = varchar`` is not an operator PostgreSQL has. Comparing the
        column as text accepts either.
        """
        col = self._version_col()
        if self._is_server_versioned():
            return cast(col, Text) == str(expected)
        return col == expected

    def update(self, values: Values, *, return_results: bool = False) -> Self:
        values = self._with_version_increment(values)
        return super().update(values, return_results=return_results)

    async def bulk_update(
        self,
        values: MultipleValues,
        *args: Any,
        on_: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        col = self._version_col()
        generator = self._version_generator()
        if col is not None and callable(generator):
            name = col.name
            values = [{**row, name: generator(None)} for row in values]
        await super().bulk_update(values, *args, on_=on_, **kwargs)

    async def update_if_match(
        self,
        values: SingleValue,
        *args: _ColumnExpressionArgument[bool],
        expected_version: Any,
        raise_on_mismatch: bool = False,
        **kwargs: Any,
    ) -> VersionedModel | None:
        """Update with ``WHERE version_col = expected_version``.

        Returns the updated model, or ``None`` if no row matched (the
        version was stale or the row is gone). Raises
        :class:`ConcurrentModificationError` when ``raise_on_mismatch`` is
        ``True`` and no row matched.
        """
        filters = (*args, self._version_filter(expected_version))
        result = await self._update_returning(values, *filters, **kwargs)
        row = result.one_or_none()
        if row is None and raise_on_mismatch:
            msg = (
                f"{self.model.__name__} with version {expected_version!r} "
                "was modified or deleted by a concurrent transaction"
            )
            raise ConcurrentModificationError(msg)
        return row

    async def delete_if_match(
        self,
        *args: _ColumnExpressionArgument[bool],
        expected_version: Any,
        raise_on_mismatch: bool = False,
        **kwargs: Any,
    ) -> VersionedModel | None:
        """Delete with ``WHERE version_col = expected_version``.

        Returns the deleted model, or ``None`` if no row matched. Raises
        :class:`ConcurrentModificationError` when ``raise_on_mismatch`` is
        ``True`` and no row matched.
        """
        filters = (*args, self._version_filter(expected_version))
        result = await self._delete_returning(*filters, **kwargs)
        row = result.one_or_none()
        if row is None and raise_on_mismatch:
            msg = (
                f"{self.model.__name__} with version {expected_version!r} "
                "was modified or deleted by a concurrent transaction"
            )
            raise ConcurrentModificationError(msg)
        return row
