from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column
from uuid_utils.compat import uuid4, uuid7

from .types import GUID, GenerateUUID, GenerateUUIDV7, Timestamp, now


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
        server_default=now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        Timestamp(),
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
