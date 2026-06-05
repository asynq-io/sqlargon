from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column
from uuid_utils import uuid4, uuid7

from .types import GUID, GenerateUUID, Timestamp, now
from .utils import utc_now

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


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
        server_default=GenerateUUID(),
        nullable=False,
    )


class CreatedUpdatedMixin:
    created_at: Mapped[datetime] = mapped_column(
        Timestamp(),
        server_default=now(),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        Timestamp(),
        server_default=now(),
        onupdate=now(),
        default=utc_now,
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
