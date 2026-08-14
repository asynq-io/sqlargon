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
