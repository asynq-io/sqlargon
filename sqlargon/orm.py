import re
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase, declared_attr

from .mixins import (
    AuditableMixin,
    CreatedUpdatedMixin,
    IntegerAuditableMixin,
    SoftDeleteMixin,
    UUIDAuditableMixin,
    UUIDVersionedMixin,
    VersionedMixin,
    XminVersionedMixin,
)

camel_to_snake = re.compile(r"(?<!^)(?=[A-Z])")

naming_convention = {
    "ix": "ix_%(table_name)s__%(column_0_N_name)s",
    "uq": "uq_%(table_name)s__%(column_0_N_name)s",
    "ck": "ck_%(table_name)s__%(constraint_name)s",
    "fk": "fk_%(table_name)s__%(column_0_N_name)s__%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    # required in order to access columns with server defaults
    # or SQL expression defaults, after a flush, without
    # triggering an expired load
    #
    # this allows us to load attributes with a server default after
    # an INSERT, for example
    #
    # https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html#preventing-implicit-io-when-using-asyncsession
    metadata = MetaData(naming_convention=naming_convention)

    __mapper_args__: Any = {"eager_defaults": True}  # noqa: RUF012

    if TYPE_CHECKING:
        __tablename__: str
    else:

        @declared_attr.directive
        def __tablename__(cls) -> str:
            """
            By default, turn the model's camel-case class name
            into a snake-case table name. Override by providing
            an explicit `__tablename__` class property.
            """
            return camel_to_snake.sub("_", cls.__name__).lower()


ORMModel = Base

Model = TypeVar("Model", bound=Base)


class SoftDeleteBase(SoftDeleteMixin, Base):
    """Declarative base for models that are deleted by raising a tombstone.

    Inherit it instead of combining :class:`SoftDeleteMixin` with
    :class:`~sqlargon.orm.Base` by hand, so
    :class:`~sqlargon.repository.SoftDeleteRepository` can type its model::

        class User(UUIDModelMixin, SoftDeleteBase):
            name: Mapped[str] = mapped_column(sa.Unicode(255))
    """

    __abstract__ = True


SoftDeleteModel = TypeVar("SoftDeleteModel", bound=SoftDeleteBase)


class AnyVersionedBase(VersionedMixin, Base):
    """Declarative base shared by every versioning strategy.

    It declares no version column of its own; it exists so
    :class:`~sqlargon.repository.VersionedRepository` can type its model
    against any strategy rather than against the UUID one alone.
    """

    __abstract__ = True


class VersionedBase(UUIDVersionedMixin, AnyVersionedBase):
    """Declarative base for models versioned with a UUID column.

    Inherit it instead of combining :class:`UUIDVersionedMixin` with
    :class:`~sqlargon.orm.Base` by hand, so
    :class:`~sqlargon.repository.VersionedRepository` can type its model::

        class User(UUIDModelMixin, VersionedBase):
            name: Mapped[str] = mapped_column(sa.Unicode(255))
    """

    __abstract__ = True


class XminVersionedBase(XminVersionedMixin, AnyVersionedBase):
    """Declarative base for PostgreSQL models versioned via ``xmin``.

    Only works on PostgreSQL — the ``xmin`` system column does not exist
    on other backends.
    """

    __abstract__ = True


VersionedModel = TypeVar("VersionedModel", bound=AnyVersionedBase)


class AnyAuditableBase(
    AuditableMixin, CreatedUpdatedMixin, SoftDeleteBase, AnyVersionedBase
):
    """Declarative base shared by every append-only versioning strategy.

    It declares no version column of its own -- inherit
    :class:`AuditableBase` or :class:`UUIDAuditableBase`.

    ``created_at`` timestamps the version rather than the entity, and
    ``updated_at`` always equals it, because a row of an append-only table
    is never updated.
    """

    __abstract__ = True


class AuditableBase(IntegerAuditableMixin, AnyAuditableBase):
    """Declarative base for append-only models with counted versions.

    The version column joins the primary key, so a model combining this with
    :class:`~sqlargon.mixins.UUIDModelMixin` is keyed by ``(id, version)``
    and its entity key is derived as ``(id,)``::

        class Article(UUIDModelMixin, AuditableBase):
            title: Mapped[str] = mapped_column(sa.Unicode(255))
    """

    __abstract__ = True


class UUIDAuditableBase(UUIDAuditableMixin, AnyAuditableBase):
    """Declarative base for append-only models versioned by UUIDv7.

    The counterpart of :class:`AuditableBase` for writers that cannot
    coordinate on a counter.
    """

    __abstract__ = True


AuditableModel = TypeVar("AuditableModel", bound=AnyAuditableBase)
