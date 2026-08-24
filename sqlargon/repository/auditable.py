from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa

from sqlargon.mixins import AuditableMixin
from sqlargon.orm import AuditableModel
from sqlargon.query_builder import Option

from .base import _as_result
from .soft_delete import SoftDeleteRepository
from .versioned import VersionedRepository

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from sqlalchemy import Result, ScalarResult
    from sqlalchemy.sql._typing import _ColumnExpressionArgument
    from typing_extensions import Self, Unpack

    from sqlargon.typing import (
        MultipleValues,
        OnConflictOptions,
        Params,
        SingleValue,
        Values,
    )

__all__ = ["AppendOnlyError", "AuditableRepository"]


class AppendOnlyError(RuntimeError):
    """A statement would have rewritten a row of an append-only table."""


@dataclass(slots=True, frozen=True)
class _Append:
    """An append a builder has staged but not yet turned into a statement.

    It is held rather than compiled straight away so ``.filter(...)`` keeps
    narrowing the select the append reads from, exactly as it would narrow an
    ordinary ``UPDATE``.
    """

    values: SingleValue
    tombstone: bool
    return_results: bool


class AuditableRepository(
    SoftDeleteRepository[AuditableModel],
    VersionedRepository[AuditableModel],
    abstract=True,
):
    """Repository that appends a new version instead of updating a row.

    The table *is* the audit log: every write appends a row carrying the next
    ``version`` of the same entity, and nothing is ever rewritten. Reads are
    scoped to the newest live version, so the usual methods keep their usual
    meaning while the history stays underneath::

        class Article(UUIDModelMixin, AuditableBase): ...


        class ArticleRepository(AuditableRepository[Article]): ...


        articles = ArticleRepository()

        article = await articles.create(title="draft")  # version 1
        await articles.update_one({"title": "final"}, Article.id == article.id)

        await articles.get(id=article.id)  # version 2
        await articles.history(id=article.id)  # versions 1 and 2
        await articles.remove(Article.id == article.id)  # appends version 3,
        await articles.list()  # tombstoned, so the entity is gone from reads
        await articles.versions().count()  # but all three rows are still there

    The entity is identified by :meth:`~sqlargon.mixins.AuditableMixin.audit_key`
    -- the primary key minus ``version`` -- so ``count()`` counts entities
    while ``versions().count()`` counts rows. Deletion appends a tombstoned
    version rather than removing anything, which makes ``remove``,
    ``delete_one`` and ``delete_many`` all recoverable through
    :meth:`~sqlargon.repository.SoftDeleteRepository.restore`.

    :meth:`update` and :meth:`delete` still build a statement, so the fluent
    form works unchanged -- it is an ``INSERT ... SELECT`` reading the current
    heads and writing their successors, which makes an append one statement
    rather than a read followed by a write::

        await articles.update({"title": "final"}).filter(Article.id == aid)

    Because the version is part of the primary key, two writers deriving the
    same successor collide there rather than one of them silently winning.
    :meth:`~sqlargon.repository.VersionedRepository.update_if_match` and
    ``delete_if_match`` are inherited and land on the append path, giving the
    cheaper check first::

        await articles.update_if_match(
            {"title": "final"},
            Article.id == article.id,
            expected_version=1,
            raise_on_mismatch=True,
        )

    Only :meth:`upsert` is refused: resolving a conflict by rewriting the
    conflicting row is the one thing an append-only table cannot do, and
    :meth:`create_or_update` and :meth:`bulk_create_or_update` express the
    intent behind it.

    The model type variable is bound to
    :class:`~sqlargon.orm.AnyAuditableBase`, so a type checker rejects a model
    that cannot be audited. At runtime the looser
    :class:`~sqlargon.mixins.AuditableMixin` is enough; anything else raises
    ``TypeError`` on subclassing.
    """

    __slots__ = ("all_versions", "as_of", "pending")

    def __init__(self) -> None:
        super().__init__()
        self.all_versions = False
        self.as_of: datetime | None = None
        self.pending: _Append | None = None

    def __init_subclass__(cls, *, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(abstract=abstract, **kwargs)
        if abstract:
            return
        if not cls.model.audit_key():
            msg = (
                f"{cls.model.__name__} is keyed by its version alone, so "
                f"{cls.__name__} could not tell one entity from another; put "
                "the columns identifying the entity in its primary key"
            )
            raise TypeError(msg)

    @classmethod
    def _required_mixin(cls) -> tuple[type, str]:
        return (
            AuditableMixin,
            "AuditableMixin (IntegerAuditableMixin or UUIDAuditableMixin)",
        )

    # --- scoping ---

    @property
    def _scope(self) -> _ColumnExpressionArgument[bool] | None:
        tombstone = super()._scope
        if self.all_versions:
            return tombstone
        latest = self.model.is_latest(before=self.as_of)
        if tombstone is None:
            return latest
        return sa.and_(latest, tombstone)

    def copy(self, query: Any) -> Self:
        clone = super().copy(query)
        clone.all_versions = self.all_versions
        clone.as_of = self.as_of
        clone.pending = self.pending
        return clone

    def versions(self) -> Self:
        """Return a copy covering every version of every entity.

        The scope holds for every statement the copy builds, so
        ``versions().count()`` counts rows rather than entities and
        ``versions().filter(...)`` searches the whole history.
        """
        clone = self.copy(self._query)
        clone.all_versions = True
        clone.include_deleted = True
        clone.deleted_only = False
        return clone

    def at(self, timestamp: datetime) -> Self:
        """Return a copy reading the state as it stood at ``timestamp``.

        Each entity resolves to the newest version recorded up to that moment,
        and one already tombstoned by then stays hidden, exactly as it would
        have been at the time.
        """
        clone = self.copy(self._query)
        clone.as_of = timestamp
        return clone

    # --- history ---

    async def history(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Sequence[AuditableModel]:
        """Every version of the matched entities, oldest first."""
        model = self.model
        order_by = (
            *(getattr(model, name) for name in model.audit_key()),
            model.version,
        )
        return (
            await self.versions()
            .select()
            .filter(*args, **kwargs)
            .order_by(*order_by)
            .all()
        )

    async def get_version(
        self, version: Any, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> AuditableModel | None:
        """One exact version of the matched entity, tombstoned or not."""
        return (
            await self.versions()
            .select()
            .filter(self.model.version == version, *args, **kwargs)
            .one_or_none()
        )

    # --- staging an append ---

    def update(self, values: Values, *, return_results: bool = False) -> Self:
        """Stage the next version of every entity the statement goes on to match.

        Nothing is rewritten: what this builds is an ``INSERT ... SELECT``
        over the current heads, so ``.filter(...)`` narrows the *source*
        select exactly as it would narrow an ``UPDATE``::

            await repo.update({"title": "final"}).filter(Article.id == aid)
        """
        if not isinstance(values, MappingABC):
            name = type(self).__name__
            msg = (
                f"{name} appends one version per matched entity and so takes "
                "a single mapping; use bulk_update to give each entity its "
                "own columns"
            )
            raise AppendOnlyError(msg)
        clone = self.select()
        clone.pending = _Append(values, tombstone=False, return_results=return_results)
        return clone

    def delete(self, *, return_results: bool = False) -> Self:
        """Stage a tombstoned version of every entity the statement matches."""
        clone = self.select()
        clone.pending = _Append({}, tombstone=True, return_results=return_results)
        return clone

    def _append_statement(self, pending: _Append) -> Any:
        """Compile the staged append against the select built so far."""
        table = self.model.__table__
        carried = self._carried_columns()
        columns: list[str] = []
        selected: list[Any] = []
        for column in table.columns:
            name = column.name
            if name in {"version", "tombstone"}:
                continue
            if name in pending.values:
                value = pending.values[name]
                columns.append(name)
                selected.append(
                    value
                    # a mapped attribute is not a ClauseElement but resolves
                    # to one, and either may stand in for a literal
                    if isinstance(value, sa.ClauseElement)
                    or hasattr(value, "__clause_element__")
                    else sa.literal(value, column.type)
                )
            elif name in carried:
                columns.append(name)
                selected.append(column)
        columns += ["version", "tombstone"]
        selected += [
            self.model.next_version_expression(),
            sa.literal(pending.tombstone, table.c.tombstone.type),
        ]
        statement = sa.insert(self.model).from_select(
            columns, self.query.with_only_columns(*selected)
        )
        if pending.return_results and self.qb.supports(Option.RETURNING):
            return statement.returning(self.model)
        return statement

    async def execute(
        self,
        params: Params | None = None,
        *,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> Result:
        pending = self.pending
        if pending is None:
            return await super().execute(params, read_only=read_only, **kwargs)
        statement = self._append_statement(pending)
        if not pending.return_results or self.qb.supports(Option.RETURNING):
            return await self.execute_query(
                statement, params, read_only=read_only, **kwargs
            )
        # a backend without RETURNING has to find the appended rows again,
        # which it can: an append leaves its entity's newest version behind
        elements = self.model.audit_key()
        columns = tuple(self._column(name) for name in elements)
        async with self.session():
            identities = [
                dict(zip(elements, tuple(row), strict=True))
                for row in (
                    await self.execute_query(self.query.with_only_columns(*columns))
                ).all()
            ]
            await self.execute_query(statement, params, **kwargs)
            if not identities:
                return _as_result([])
            appended = await self._fetch(
                sa.and_(
                    self._identity_filter(identities, elements),
                    self.model.is_latest(),
                )
            )
            return _as_result(appended.all())

    def stream(
        self,
        params: Params | None = None,
        *,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> Any:
        pending = self.pending
        query = self.query if pending is None else self._append_statement(pending)
        return self.stream_query(query, params, read_only=read_only, **kwargs)

    async def _update_returning(
        self, values: Values, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> ScalarResult[AuditableModel]:
        return (
            await self.update(values, return_results=True)
            .filter(*args, **kwargs)
            .scalars()
        )

    async def _delete_returning(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> ScalarResult[AuditableModel]:
        return await self.delete(return_results=True).filter(*args, **kwargs).scalars()

    # --- building a version client side, where one statement cannot ---

    @classmethod
    def _next_version(cls, current: Any) -> Any:
        """The version superseding ``current``, per the model's strategy.

        The counterpart of
        :meth:`~sqlargon.mixins.AuditableMixin.next_version_expression` for
        the paths that carry a different set of values per entity and so
        cannot be one ``INSERT ... SELECT``.
        """
        return cls._version_generator()(current)

    @classmethod
    def _carried_columns(cls) -> set[str]:
        """The columns an appended version inherits from the one it supersedes.

        Everything the append derives itself is left out, so a value the
        caller did not name is carried forward while the new row still gets
        its own version and timestamps.
        """
        derived = {"version", "tombstone", "created_at", "updated_at"}
        return {c.name for c in cls.model.__table__.columns if c.name not in derived}

    def _successor(
        self, row: AuditableModel, values: SingleValue, *, tombstone: bool = False
    ) -> dict[str, Any]:
        return {
            **{name: getattr(row, name) for name in self._carried_columns()},
            **values,
            "version": self._next_version(row.version),
            "tombstone": tombstone,
        }

    def _keyed(self, rows: MultipleValues) -> tuple[str, ...]:
        elements = self.model.audit_key()
        missing = [name for name in elements if any(name not in row for row in rows)]
        if missing:
            msg = (
                f"every row has to name the entity key of {self.model.__name__} "
                f"{elements}, but {missing} is missing from at least one"
            )
            raise AppendOnlyError(msg)
        return elements

    # --- writes ---

    async def create_or_update(self, **kwargs: Any) -> AuditableModel:
        """Append the next version of an entity, creating it if it has none.

        An entity whose newest version is a tombstone is revived rather than
        refused: unlike a soft delete, the append leaves the deletion in the
        history for anyone to read.
        """
        key = self.model.audit_key()
        identity = {name: kwargs[name] for name in key if name in kwargs}
        async with self.session():
            if len(identity) == len(key):
                appended = await self.with_deleted().update_many(kwargs, **identity)
                if appended:
                    return appended[0]
            return (await self._insert_returning([kwargs])).one()

    async def bulk_create_or_update(
        self,
        values: MultipleValues,
        *,
        return_results: bool = False,
        **options: Unpack[OnConflictOptions],  # noqa: ARG002
    ) -> Any:
        """Append a version per known entity and create the rest at version 1.

        The bulk form of :meth:`create_or_update`. Conflict options are
        ignored -- an append has no conflicting row to resolve against.
        """
        rows = list(values)
        if not rows:
            return [] if return_results else _as_result([])
        elements = self._keyed(rows)
        async with self.session():
            heads = (
                await self.with_deleted()
                .select()
                .filter(self._identity_filter(rows, elements))
                .all()
            )
            by_identity = {
                tuple(getattr(head, name) for name in elements): head for head in heads
            }
            appended, created = [], []
            for row in rows:
                head = by_identity.get(self._identity(row, elements))
                if head is None:
                    created.append(dict(row))
                else:
                    appended.append(self._successor(head, row))
            # an appended row names every carried column while a created one
            # names only what the caller gave, so the two cannot share an
            # executemany -- a column missing from one row of a multi-values
            # INSERT has no bound parameter to render
            if return_results:
                return [
                    row
                    for batch in (appended, created)
                    if batch
                    for row in (await self._insert_returning(batch)).all()
                ]
            result: Result = _as_result([])
            for batch in (appended, created):
                if batch:
                    result = await self.insert(batch).execute()
            return result

    async def bulk_update(
        self,
        values: MultipleValues,
        *args: Any,
        on_: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Append one version per row, matching entities on ``on_``.

        ``on_`` defaults to the entity key rather than the primary key, since
        naming the version would pin every row to the one it already has.
        """
        rows = list(values)
        if not rows:
            return
        elements = tuple(on_) if on_ else self._keyed(rows)
        async with self.session():
            current = await (
                self.select()
                .filter(self._identity_filter(rows, elements), *args, **kwargs)
                .all()
            )
            by_identity = {
                tuple(getattr(row, name) for name in elements): row for row in current
            }
            appended = [
                self._successor(head, row)
                for row in rows
                if (head := by_identity.get(self._identity(row, elements))) is not None
            ]
            if appended:
                await self.insert(appended).execute()

    async def purge(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        """Physically delete every superseded version, keeping the newest.

        The only method here that destroys history -- for retention, not for
        deletion, which :meth:`remove` records instead. A version something
        still points at through
        :func:`~sqlargon.audit.version_foreign_key` is protected by that key.
        """
        elements = (*self.model.audit_key(), "version")
        query = self.qb.filter(
            self.qb.select(*(self._column(name) for name in elements)).where(
                sa.not_(self.model.is_latest())
            ),
            *args,
            **kwargs,
        )
        async with self.session():
            superseded = [
                dict(zip(elements, tuple(row), strict=True))
                for row in (await self.execute_query(query)).all()
            ]
            if not superseded:
                return
            await self.versions().hard_delete(
                self._identity_filter(superseded, elements)
            )

    # --- the one statement an append-only table cannot serve ---

    def upsert(
        self,
        values: Values,  # noqa: ARG002
        *,
        return_results: bool = False,  # noqa: ARG002
        **options: Unpack[OnConflictOptions],  # noqa: ARG002
    ) -> Self:
        msg = (
            f"{type(self).__name__} cannot resolve a conflict by rewriting the "
            "conflicting row; use create_or_update or bulk_create_or_update to "
            "append the next version instead"
        )
        raise AppendOnlyError(msg)
