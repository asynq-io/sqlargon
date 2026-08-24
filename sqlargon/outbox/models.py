from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon.mixins import CreatedUpdatedMixin, UUIDModelMixin
from sqlargon.orm import Base
from sqlargon.types import JSON, Timestamp, now
from sqlargon.utils import utc_now


class OutboxEvent(UUIDModelMixin, CreatedUpdatedMixin, Base):
    """A CloudEvent awaiting publication.

    ``id`` is the CloudEvent ``id`` and ``created_at`` its ``time``; the
    remaining CloudEvent attributes map one to one, and ``specversion`` is
    left to the publisher as the constant it is. ``attributes`` holds the
    extension attributes -- the ones the CloudEvents spec leaves to the
    application -- while ``headers`` holds the broker's transport headers.
    Every column carries a server default as well as a client one, so a row
    inserted without naming them is still complete.
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        sa.Index(
            "idx_outbox_events_pending", "published_at", "available_at", "created_at"
        ),
    )

    topic: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    type: Mapped[str] = mapped_column(sa.String(255), nullable=False)
    source: Mapped[str | None] = mapped_column(sa.String(255), nullable=True)
    data: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    attributes: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    headers: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(Timestamp(), nullable=True)
    available_at: Mapped[datetime] = mapped_column(
        Timestamp(), nullable=False, default=utc_now, server_default=now()
    )
    attempts: Mapped[int] = mapped_column(
        sa.Integer(), nullable=False, default=0, server_default=sa.text("0")
    )
    last_error: Mapped[str | None] = mapped_column(sa.Text(), nullable=True)
