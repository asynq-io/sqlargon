from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon.mixins import CreatedUpdatedMixin, UUIDModelMixin
from sqlargon.orm import Base
from sqlargon.types import JSON, Timestamp


class CronTask(UUIDModelMixin, CreatedUpdatedMixin, Base):
    """A scheduled task persisted per namespace."""

    __tablename__ = "cron_tasks"
    __table_args__ = (
        sa.UniqueConstraint("namespace", "name"),
        sa.Index("idx_cron_tasks_namespace_next_run_at", "namespace", "next_run_at"),
    )

    namespace: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    schedule: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    declarative: Mapped[bool] = mapped_column(
        sa.Boolean(), nullable=False, default=False, server_default=sa.sql.false()
    )
    enabled: Mapped[bool] = mapped_column(
        sa.Boolean(), nullable=False, default=True, server_default=sa.sql.true()
    )
    next_run_at: Mapped[datetime] = mapped_column(Timestamp(), nullable=False)
    last_run_at: Mapped[datetime | None] = mapped_column(Timestamp(), nullable=True)
    args: Mapped[list[Any] | None] = mapped_column(JSON(), nullable=True)
    kwargs: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
