from __future__ import annotations

from operator import attrgetter
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast, overload

import sqlalchemy as sa
from pydantic_core import to_jsonable_python
from sqlalchemy.engine.result import IteratorResult, SimpleResultMetaData

from sqlargon.mixins import CreatedUpdatedMixin
from sqlargon.orm import Model
from sqlargon.repository import SQLAlchemyRepository

from .config import Operation, OutboxConfig
from .models import OutboxEvent

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Sequence
    from datetime import datetime, timedelta
    from uuid import UUID

    from sqlalchemy import CursorResult, Result, ScalarResult
    from sqlalchemy.orm import Mapper
    from sqlalchemy.sql._typing import _ColumnExpressionArgument
    from typing_extensions import Unpack

    from sqlargon.typing import MultipleValues, OnConflictOptions, SingleValue, Values


def _as_result(rows: Sequence[object]) -> IteratorResult[Any]:
    """Re-wrap already fetched rows as a single-column result.

    The capture hooks have to consume the result of the write they wrap in
    order to build the events, so the rows are handed back in a fresh result
    rather than the exhausted one.
    """
    metadata = SimpleResultMetaData(("value",))
    return IteratorResult(metadata, iter([(row,) for row in rows]))


def _as_scalars(rows: Sequence[Model]) -> ScalarResult[Model]:
    return cast("ScalarResult[Model]", _as_result(rows).scalars())


class OutboxEventRepository(SQLAlchemyRepository[OutboxEvent]):
    """Repository for :class:`OutboxEvent` rows, used by the relay."""

    async def claim_pending(
        self,
        now: datetime,
        *,
        lease: timedelta,
        limit: int = 100,
        max_attempts: int | None = None,
    ) -> Sequence[OutboxEvent]:
        """Claim the events due at ``now`` and return them in write order.

        Due rows are locked with ``FOR UPDATE SKIP LOCKED`` and leased -- their
        ``available_at`` pushed to ``now + lease`` and ``attempts`` bumped --
        within the same transaction, so concurrent relays never claim the same
        event and one that dies mid-batch leaves nothing wedged.

        Events that already failed ``max_attempts`` times are left alone; they
        keep their ``last_error`` for inspection.
        """
        filters: list[_ColumnExpressionArgument[bool]] = [
            OutboxEvent.published_at.is_(None),
            OutboxEvent.available_at <= now,
        ]
        if max_attempts is not None:
            filters.append(OutboxEvent.attempts < max_attempts)
        async with self.session():
            events = await (
                self.select(with_for_update={"skip_locked": True})
                .filter(*filters)
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
                .limit(limit)
                .all()
            )
            for event in events:
                event.attempts += 1
                event.available_at = now + lease
            return events

    async def mark_published(
        self, ids: Collection[UUID], published_at: datetime
    ) -> None:
        """Mark the events as published, clearing any recorded error."""
        if not ids:
            return
        await (
            self.update({"published_at": published_at, "last_error": None})
            .filter(OutboxEvent.id.in_(list(ids)))
            .execute()
        )

    async def mark_failed(self, event_id: UUID, error: str, retry_at: datetime) -> None:
        """Record why publishing failed and when to try again."""
        await (
            self.update({"last_error": error, "available_at": retry_at})
            .filter(OutboxEvent.id == event_id)
            .execute()
        )

    async def purge(self, published_before: datetime) -> int:
        """Delete the events published before the cutoff and count them."""
        result = await (
            self.delete()
            .filter(
                OutboxEvent.published_at.is_not(None),
                OutboxEvent.published_at < published_before,
            )
            .execute()
        )
        return cast("CursorResult[Any]", result).rowcount

    async def pending_count(self) -> int:
        """How many events are still waiting to be published."""
        return await self.count(OutboxEvent.published_at.is_(None))


class OutboxRepository(SQLAlchemyRepository[Model], abstract=True):
    """Repository that records every write as an event, in the same transaction.

    Each statement it writes is followed by an insert into ``outbox_events``
    on the same session, so an event can neither be lost by a rollback nor
    published for a row that never committed::

        class User(UUIDModelMixin, CreatedUpdatedMixin, Base): ...


        class UserRepository(OutboxRepository[User]):
            outbox = OutboxConfig(topic="users", exclude={"password"})


        await UserRepository().create(name="John", password=hashed)
        # -> one outbox_events row, type "user.created", password left out

    The model must carry
    :class:`~sqlargon.mixins.CreatedUpdatedMixin`, whose ``is_new`` is what
    tells an insert from an update: an upsert writes some rows and updates
    others in one statement, and each is recorded as what it turned out to
    be. What those writes look like as events is otherwise settled by the
    repository rather than by the model, the way
    :attr:`~sqlargon.repository.SQLAlchemyRepository.on_conflict` is, so two
    repositories over the same model can publish differently and a model
    reached through an ordinary repository records nothing. The payload is the
    full row, minus whatever :attr:`outbox` excludes.

    Reads back are needed to build the events, so ``remove`` and
    ``delete_many`` go through a RETURNING statement here rather than a bare
    DELETE. On backends without RETURNING the repository's existing fallback
    (select the identities, write, re-fetch) supplies the rows, so nothing
    dialect-specific is involved.
    """

    event_repository_class: ClassVar[type[OutboxEventRepository]] = (
        OutboxEventRepository
    )
    outbox: ClassVar[OutboxConfig] = OutboxConfig()

    def __init_subclass__(cls, *, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(abstract=abstract, **kwargs)
        if not abstract and not issubclass(cls.model, CreatedUpdatedMixin):
            msg = (
                f"{cls.model.__name__} must inherit from CreatedUpdatedMixin "
                f"to be used with {cls.__name__}"
            )
            raise TypeError(msg)

    @property
    def events(self) -> OutboxEventRepository:
        """A repository for the event table, bound to the same database."""
        return self.event_repository_class().using(db=self.db)

    @property
    def topic(self) -> str:
        """The topic every event of this repository is published on."""
        return self.outbox.topic or self.model.__tablename__

    @property
    def event_types(self) -> dict[Operation, str]:
        """The event type of each recorded operation, and only of those."""
        prefix = self.outbox.type_prefix or self.model.__tablename__
        return {
            operation: f"{prefix}.{operation.value}"
            for operation in self.outbox.operations
        }

    @property
    def payload_columns(self) -> tuple[tuple[str, str], ...]:
        """``(column name, attribute key)`` pairs making up the payload.

        Keyed by column name rather than by attribute key, so the payload
        speaks the database's names rather than the mapper's.
        """
        mapper: Mapper[Model] = sa.inspect(self.model)
        includes_column = self.outbox.includes_column
        return tuple(
            (column.name, key)
            for key, column in mapper.columns.items()
            if includes_column(column.name)
        )

    @property
    def attribute_sources(self) -> tuple[tuple[str, Callable[[Any], Any]], ...]:
        """``(attribute name, getter)`` pairs for the extra CloudEvent attributes.

        A source named as a string is resolved once, here, into the getter
        reading it off the written row.
        """
        return tuple(
            (name, source if callable(source) else attrgetter(source))
            for name, source in self.outbox.attributes.items()
        )

    def _operation_of(self, row: Model) -> Operation:
        """Whether the write inserted the row or updated one already there.

        An upsert does both in one statement, and only the row itself knows
        which it was: ``created_at`` is the value it was inserted with, so a
        row whose ``updated_at`` still matches it has just been created.
        """
        is_new = cast("CreatedUpdatedMixin", row).is_new
        return Operation.CREATED if is_new else Operation.UPDATED

    def _build_events(
        self, written: Sequence[tuple[Model, Operation]]
    ) -> list[dict[str, Any]]:
        """One event per written row, with a payload of JSON primitives.

        The payload is reduced to primitives here rather than left to the
        engine's ``json_serializer``: a :class:`~sqlargon.Database` built
        straight from a URL carries the standard library's, which knows
        neither UUIDs nor datetimes. The extra attributes are read here too,
        while the context the write ran in is still the current one.
        """
        columns = self.payload_columns
        sources = self.attribute_sources
        event_types = self.event_types
        return [
            {
                "topic": self.topic,
                "type": event_type,
                "source": self.outbox.source,
                "data": to_jsonable_python(
                    {name: getattr(row, key) for name, key in columns}
                ),
                "attributes": to_jsonable_python(
                    {name: source(row) for name, source in sources}
                )
                if sources
                else None,
            }
            for row, operation in written
            if (event_type := event_types.get(operation)) is not None
        ]

    async def _record(
        self, rows: Sequence[Model], operation: Operation | None = None
    ) -> Sequence[Model]:
        """Record one event per row, on the session the write ran on.

        Without an ``operation`` every row reports its own, which is what an
        insert that may have updated instead needs.
        """
        events = self._build_events(
            [
                (row, operation if operation is not None else self._operation_of(row))
                for row in rows
            ]
        )
        if events:
            await self.events.bulk_create(events, ignore_conflicts=False)
        return rows

    async def _insert_returning(
        self,
        values: MultipleValues,
        *,
        do: Literal["ignore", "update"] | None = None,
        **options: Unpack[OnConflictOptions],
    ) -> ScalarResult[Model]:
        async with self.session():
            result = await super()._insert_returning(values, do=do, **options)
            return _as_scalars(await self._record(result.all()))

    async def _update_returning(
        self, values: Values, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> ScalarResult[Model]:
        async with self.session():
            result = await super()._update_returning(values, *args, **kwargs)
            return _as_scalars(await self._record(result.all(), Operation.UPDATED))

    async def _delete_returning(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> ScalarResult[Model]:
        async with self.session():
            result = await super()._delete_returning(*args, **kwargs)
            return _as_scalars(await self._record(result.all(), Operation.DELETED))

    async def remove(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        """Delete the matched rows, recording one event per row."""
        await self._delete_returning(*args, **kwargs)

    async def delete_many(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        """Delete the matched rows, recording one event per row."""
        await self._delete_returning(*args, **kwargs)

    @overload
    async def bulk_create(
        self,
        values: MultipleValues,
        *,
        ignore_conflicts: bool = ...,
        return_results: Literal[False] = ...,
        **options: Unpack[OnConflictOptions],
    ) -> None: ...

    @overload
    async def bulk_create(
        self,
        values: MultipleValues,
        *,
        ignore_conflicts: bool = ...,
        return_results: Literal[True],
        **options: Unpack[OnConflictOptions],
    ) -> Sequence[Model]: ...

    async def bulk_create(
        self,
        values: MultipleValues,
        *,
        ignore_conflicts: bool = True,
        return_results: bool = False,
        **options: Unpack[OnConflictOptions],
    ) -> Sequence[Model] | None:
        """Insert the rows, recording one event per written row."""
        result = await self._insert_returning(
            values, do="ignore" if ignore_conflicts else None, **options
        )
        return result.all() if return_results else None

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
        """Upsert the rows, recording one event per written row.

        Without ``return_results`` the written rows are still handed back, in a
        result built from them rather than the cursor's -- so unlike the base
        repository's, it carries no ``rowcount``.
        """
        rows = (await self._insert_returning(values, do="update", **options)).all()
        return rows if return_results else _as_result(rows)

    async def bulk_update(
        self,
        values: MultipleValues,
        *args: Any,
        on_: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Update many rows in one statement, recording one event per row.

        An executemany cannot return rows, so the updated rows are read back
        by the keys the statement matched on.
        """
        on_ = on_ or self._get_default_index_elements()
        elements = tuple(on_)
        async with self.session():
            await super().bulk_update(values, *args, on_=on_, **kwargs)
            rows = await self._fetch(
                self._identity_filter(cast("Sequence[SingleValue]", values), elements)
            )
            await self._record(rows.all(), Operation.UPDATED)
