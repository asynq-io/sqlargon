from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import FetchedValue
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, declarative_mixin

from .types import GUID, GenerateUUID, Timestamp, now
from .utils import utc_now


@declarative_mixin
class UUIDModelMixin:
    id: Mapped[UUID] = sa.Column(
        GUID(),
        primary_key=True,
        default=uuid4,
        server_default=GenerateUUID(),
        nullable=False,
    )


@declarative_mixin
class CreatedUpdatedMixin:
    created_at: Mapped[datetime] = sa.Column(
        Timestamp(),
        server_default=now(),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = sa.Column(
        Timestamp(),
        server_default=now(),
        onupdate=now(),
        default=utc_now,
        nullable=False,
        server_onupdate=FetchedValue(),
    )

    @hybrid_property
    def is_new(self):
        return self.created_at == self.updated_at


@declarative_mixin
class SoftDeleteMixin:
    tombstone: Mapped[bool] = sa.Column(
        sa.Boolean(), nullable=False, default=False, server_default=sa.sql.false()
    )

    @hybrid_property
    def not_deleted(self):
        return not self.tombstone

    @not_deleted.expression  # type: ignore
    def not_deleted(cls):
        return cls.tombstone.is_(False)

    @hybrid_property
    def is_deleted(self):
        return self.tombstone

    @is_deleted.expression  # type: ignore
    def is_deleted(cls):
        return cls.tombstone.is_(True)
