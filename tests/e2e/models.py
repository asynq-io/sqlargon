"""Models and repositories the e2e suite exercises.

Declared at module level so ``--count=3`` re-runs reuse them, and exported as
:data:`TABLES` so only these tables are created on a backend -- the shared
declarative metadata also holds every model the unit suite declares.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon import Base, SoftDeleteBase, SoftDeleteRepository, SQLAlchemyRepository
from sqlargon.mixins import CreatedUpdatedMixin, UUIDModelMixin
from sqlargon.types import GUID, JSON, GenerateUUID, GenerateUUIDV7, Timestamp, now
from sqlargon.types.pydantic import Pydantic
from sqlargon.typing import OnConflictOptions


class Address(BaseModel):
    """Payload of the pydantic-typed column of :class:`Document`."""

    city: str
    zip_code: str


class User(UUIDModelMixin, CreatedUpdatedMixin, Base):
    # a 64 character unique column stays within the MySQL index key limit
    __tablename__ = "e2e_user"

    name: Mapped[str] = mapped_column(sa.Unicode(64), unique=True)
    last_name: Mapped[str | None] = mapped_column(sa.Unicode(64), nullable=True)
    score: Mapped[int] = mapped_column(
        sa.Integer, default=0, server_default=sa.text("0")
    )


class Document(UUIDModelMixin, Base):
    __tablename__ = "e2e_document"

    tags: Mapped[list[str]] = mapped_column(JSON())
    payload: Mapped[dict[str, Any]] = mapped_column(JSON())
    address: Mapped[Address | None] = mapped_column(Pydantic(Address), nullable=True)


class SoftUser(UUIDModelMixin, SoftDeleteBase):
    __tablename__ = "e2e_soft_user"

    name: Mapped[str] = mapped_column(sa.Unicode(64), unique=True)


class ServerDefaults(Base):
    """A row whose every column is filled by the server."""

    __tablename__ = "e2e_server_defaults"

    id: Mapped[UUID] = mapped_column(
        GUID(), primary_key=True, server_default=GenerateUUID()
    )
    uuid_v7: Mapped[UUID] = mapped_column(
        GUID(), nullable=False, server_default=GenerateUUIDV7()
    )
    created_at: Mapped[datetime] = mapped_column(
        Timestamp(), nullable=False, server_default=now()
    )


class UserRepository(SQLAlchemyRepository[User]):
    default_order_by = User.name


class UserByNameRepository(UserRepository):
    """Resolves conflicts on the unique name rather than on the primary key."""

    @property
    def on_conflict(self) -> OnConflictOptions:
        return {"index_elements": {"name"}, "set_": {"last_name", "score"}}


class ExcludingUserRepository(UserRepository):
    """Leaves the score of a conflicting row untouched on upsert."""

    @property
    def on_conflict(self) -> OnConflictOptions:
        return {
            "index_elements": {"id"},
            "set_": {"name", "score"},
            "exclude_set": {"score"},
        }


class DocumentRepository(SQLAlchemyRepository[Document]):
    pass


class SoftUserRepository(SoftDeleteRepository[SoftUser]):
    default_order_by = SoftUser.name


class SoftUserByNameRepository(SoftUserRepository):
    """Resolves conflicts on the unique name rather than on the primary key."""

    @property
    def on_conflict(self) -> OnConflictOptions:
        return {"index_elements": {"name"}, "set_": {"name"}}


def _tables(*models: type[Base]) -> tuple[sa.Table, ...]:
    return tuple(Base.metadata.tables[model.__tablename__] for model in models)


#: Tables every backend can hold; the only ones the e2e suite creates.
TABLES: tuple[sa.Table, ...] = _tables(User, Document, SoftUser)

#: Tables whose DDL carries a server side UUID default, which not every
#: backend accepts -- see :attr:`~tests.e2e.backends.Backend.server_side_uuid`.
SERVER_DEFAULT_TABLES: tuple[sa.Table, ...] = _tables(ServerDefaults)
