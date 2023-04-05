import sqlalchemy as sa
from sqlalchemy import FetchedValue
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import declared_attr

from .function_elements import GenerateUUID
from .types import GUID


class UUIDModelMixin:
    @declared_attr
    def id(cls):
        return sa.Column(
            GUID(), primary_key=True, server_default=GenerateUUID(), nullable=False
        )


class CreatedUpdatedMixin:
    @declared_attr
    def created_at(cls):
        return sa.Column(sa.DateTime, server_default=sa.func.now(), nullable=False)

    @declared_attr
    def updated_at(cls):
        return sa.Column(
            sa.DateTime,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
            server_onupdate=FetchedValue(),
        )


class SoftDeleteMixin:
    @declared_attr
    def tombstone(cls):
        return sa.Column(sa.Boolean(), default=False)

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
