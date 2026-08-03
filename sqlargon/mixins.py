from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID
from weakref import WeakKeyDictionary

import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column
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

    updated_at: Mapped[datetime] = mapped_column(
        Timestamp(),
        default=_statement_utc_now,
        server_default=now(),
        onupdate=now(),
        nullable=False,
        server_onupdate=now(),
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
