from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID
from weakref import WeakKeyDictionary

import sqlalchemy as sa
from sqlalchemy import FetchedValue
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, declared_attr, mapped_column
from uuid_utils.compat import uuid4, uuid7

from .types import GUID, GenerateUUID, GenerateUUIDV7, Timestamp, now
from .utils import utc_now

if TYPE_CHECKING:
    from sqlalchemy.engine.default import DefaultExecutionContext

_statement_timestamps: WeakKeyDictionary[object, datetime] = WeakKeyDictionary()


def _statement_utc_now(context: "DefaultExecutionContext") -> datetime:
    """One shared timestamp per statement, mirroring server-side ``now()``.

    ``created_at`` and ``updated_at`` both use it as their client-side
    default, so freshly inserted rows always satisfy ``is_new``.
    """
    try:
        return _statement_timestamps[context]
    except KeyError:
        timestamp = _statement_timestamps[context] = utc_now()
        return timestamp


class UUIDModelMixin:
    id: Mapped[UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid4,
        server_default=GenerateUUID(),
        nullable=False,
    )


class UUIDV7ModelMixin:
    id: Mapped[UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid7,
        server_default=GenerateUUIDV7(),
        nullable=False,
    )


class CreatedUpdatedMixin:
    created_at: Mapped[datetime] = mapped_column(
        Timestamp(),
        default=_statement_utc_now,
        server_default=now(),
        nullable=False,
    )

    # onupdate uses the client clock like the insert defaults, so ORM rows
    # never mix clock sources and ``updated_at >= created_at`` holds even
    # under client/server clock skew
    updated_at: Mapped[datetime] = mapped_column(
        Timestamp(),
        default=_statement_utc_now,
        server_default=now(),
        onupdate=utc_now,
        nullable=False,
    )

    @hybrid_property
    def is_new(self) -> bool:
        return self.created_at == self.updated_at


class SoftDeleteMixin:
    tombstone: Mapped[bool] = mapped_column(
        sa.Boolean(), nullable=False, default=False, server_default=sa.sql.false()
    )

    @hybrid_property
    def not_deleted(self) -> bool:
        return not self.tombstone

    @not_deleted.inplace.expression
    @classmethod
    def _not_deleted_expression(cls) -> sa.ColumnElement[bool]:
        return sa.not_(cls.tombstone)

    @hybrid_property
    def is_deleted(self) -> bool:
        return self.tombstone

    @is_deleted.inplace.expression
    @classmethod
    def _is_deleted_expression(cls) -> sa.ColumnElement[bool]:
        return cls.tombstone.is_(sa.true())


def _generate_version_uuid(_version: Any) -> UUID:
    """Generate a fresh UUID for the version column, ignoring the prior value."""
    return uuid4()


class VersionedMixin:
    """Marker mixin for optimistically versioned models.

    Use :class:`UUIDVersionedMixin` (backend-agnostic) or
    :class:`XminVersionedMixin` (PostgreSQL-only) — this base only
    exists so :class:`~sqlargon.repository.VersionedRepository` can
    validate its model at subclass time, the same role
    :class:`SoftDeleteMixin` plays for
    :class:`~sqlargon.repository.SoftDeleteRepository`.
    """


class UUIDVersionedMixin(VersionedMixin):
    """Backend-agnostic versioning via a UUID version column.

    Every UPDATE through
    :class:`~sqlargon.repository.VersionedRepository` replaces
    ``version_id`` with a fresh UUID; ``update_if_match`` /
    ``delete_if_match`` add a ``WHERE version_id = :expected`` guard so
    a stale row matches zero rows.

    The column uses the portable :class:`~sqlargon.types.GUID` type
    (native UUID on PostgreSQL, CHAR(36) elsewhere) with both a
    client-side ``default`` and a ``server_default`` so raw SQL inserts
    also get a version.
    """

    version_id: Mapped[UUID] = mapped_column(
        GUID(),
        nullable=False,
        default=uuid4,
        server_default=GenerateUUID(),
    )

    @declared_attr.directive
    def __mapper_args__(cls) -> dict[str, Any]:
        return {
            "eager_defaults": True,
            "version_id_col": cls.version_id,
            "version_id_generator": _generate_version_uuid,
        }


class XminVersionedMixin(VersionedMixin):
    """PostgreSQL-native versioning via the ``xmin`` system column.

    The ``xmin`` column is implicit (not in DDL); PostgreSQL changes it
    on every UPDATE, so the repository does **not** set it — only
    ``update_if_match`` / ``delete_if_match`` use it as a guard.

    The value is an ``xid``, which the driver decodes as it sees fit --
    asyncpg yields an ``int`` -- so the attribute is left untyped and the
    guard compares the column as text.

    Only works on PostgreSQL. On other backends the column does not
    exist and queries will fail.
    """

    xmin: Mapped[Any] = mapped_column(
        "xmin", sa.String, system=True, server_default=FetchedValue()
    )

    @declared_attr.directive
    def __mapper_args__(cls) -> dict[str, Any]:
        return {
            "eager_defaults": True,
            "version_id_col": cls.xmin,
            "version_id_generator": False,
        }


def _generate_version_uuid7(_current: Any) -> UUID:
    """Generate a fresh, time sortable UUID for the version column."""
    return uuid7()


def _next_integer_version(current: Any) -> int:
    """The successor of ``current``, starting the sequence at 1."""
    return 1 if current is None else current + 1


class AuditableMixin(SoftDeleteMixin, VersionedMixin):
    """Marker mixin for append-only, versioned models.

    Use :class:`IntegerAuditableMixin` (human readable 1, 2, 3 ...) or
    :class:`UUIDAuditableMixin` (time sortable UUIDv7) -- this base declares
    no version column of its own, it only carries the expressions every
    strategy shares and lets
    :class:`~sqlargon.repository.AuditableRepository` validate its model at
    subclass time.

    A row is never updated: each change appends a row holding the next
    ``version`` of the same entity, so the table *is* the audit log. The
    entity is identified by :meth:`audit_key` -- the primary key minus
    ``version`` -- and the tombstone inherited from :class:`SoftDeleteMixin`
    marks the version that records a deletion.
    """

    if TYPE_CHECKING:
        __table__: sa.Table
        version: Mapped[Any]

    @classmethod
    def audit_key(cls) -> tuple[str, ...]:
        """The columns identifying the entity: the primary key minus ``version``."""
        return tuple(
            c.name for c in cls.__table__.primary_key.columns if c.name != "version"
        )

    @classmethod
    def latest_version(cls, *, before: datetime | None = None) -> Any:
        """A scalar subquery holding the newest version of each entity.

        ``ORDER BY version DESC LIMIT 1`` rather than ``MAX(version)``: it is
        one expression for both strategies, and it does not rely on a ``max``
        aggregate existing for the UUID type of every backend. Either way the
        ordering is total -- native ``uuid`` byte order on PostgreSQL and
        lowercase hex ``CHAR(36)`` order elsewhere, both of which put a
        UUIDv7's timestamp first.

        ``before`` restricts the subquery to versions recorded up to that
        moment, which is what makes an as-of read possible.
        """
        table = cls.__table__
        alias = table.alias(f"{table.name}_latest")
        conditions = [alias.c[name] == table.c[name] for name in cls.audit_key()]
        if before is not None:
            conditions.append(alias.c.created_at <= before)
        return (
            sa.select(alias.c.version)
            .where(*conditions)
            .order_by(alias.c.version.desc())
            .limit(1)
            # the outer table appears only in the WHERE clause, so say what
            # this subquery correlates to rather than leaving it to be guessed
            .correlate(table)
            .scalar_subquery()
        )

    @classmethod
    def is_latest(cls, *, before: datetime | None = None) -> sa.ColumnElement[bool]:
        """Whether a row is the newest version of its entity."""
        return cls.version == cls.latest_version(before=before)

    @classmethod
    def next_version_expression(cls) -> Any:
        """The version superseding the one of the row being read, in SQL.

        This is what lets an append be a single ``INSERT ... SELECT`` rather
        than a read followed by a write.
        """
        raise NotImplementedError


class IntegerAuditableMixin(AuditableMixin):
    """Append-only versioning with a human readable counter: 1, 2, 3 ...

    The version is part of the primary key, so two writers deriving the same
    successor collide on it rather than one silently overwriting the other.
    """

    version: Mapped[int] = mapped_column(
        sa.Integer(),
        primary_key=True,
        nullable=False,
        default=1,
        server_default=sa.text("1"),
    )

    @declared_attr.directive
    def __mapper_args__(cls) -> dict[str, Any]:
        return {
            "eager_defaults": True,
            "version_id_col": cls.version,
            "version_id_generator": _next_integer_version,
        }

    @classmethod
    def next_version_expression(cls) -> Any:
        return cls.__table__.c.version + 1


class UUIDAuditableMixin(AuditableMixin):
    """Append-only versioning with a time sortable UUIDv7.

    The successor of a version can be minted without reading the current one,
    which suits writers that cannot coordinate. UUIDv7 is monotonic within a
    process, but two processes appending in the same millisecond can produce
    an inverted pair -- prefer :class:`IntegerAuditableMixin` when strict
    chronological ordering across writers has to hold.
    """

    version: Mapped[UUID] = mapped_column(
        GUID(),
        primary_key=True,
        nullable=False,
        default=uuid7,
        server_default=GenerateUUIDV7(),
    )

    @declared_attr.directive
    def __mapper_args__(cls) -> dict[str, Any]:
        return {
            "eager_defaults": True,
            "version_id_col": cls.version,
            "version_id_generator": _generate_version_uuid7,
        }

    @classmethod
    def next_version_expression(cls) -> Any:
        """One fresh UUIDv7, shared by every row a single statement appends.

        Sharing it is harmless: rows of one statement belong to different
        entities, so the primary key stays unique, and a value minted now
        sorts above every version already recorded.
        """
        return sa.literal(uuid7(), GUID())
