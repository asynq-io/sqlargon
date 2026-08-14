import re
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase, declared_attr

from .mixins import (
    SoftDeleteMixin,
    UUIDVersionedMixin,
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


class VersionedBase(UUIDVersionedMixin, Base):
    """Declarative base for models versioned with a UUID column.

    Inherit it instead of combining :class:`UUIDVersionedMixin` with
    :class:`~sqlargon.orm.Base` by hand, so
    :class:`~sqlargon.repository.VersionedRepository` can type its model::

        class User(UUIDModelMixin, VersionedBase):
            name: Mapped[str] = mapped_column(sa.Unicode(255))
    """

    __abstract__ = True


class XminVersionedBase(XminVersionedMixin, Base):
    """Declarative base for PostgreSQL models versioned via ``xmin``.

    Only works on PostgreSQL — the ``xmin`` system column does not exist
    on other backends.
    """

    __abstract__ = True


VersionedModel = TypeVar("VersionedModel", bound=VersionedBase)
