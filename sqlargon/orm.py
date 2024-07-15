import re
from typing import TypeVar

from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base, declared_attr

camel_to_snake = re.compile(r"(?<!^)(?=[A-Z])")


class ORMModel:
    # required in order to access columns with server defaults
    # or SQL expression defaults, after a flush, without
    # triggering an expired load
    #
    # this allows us to load attributes with a server default after
    # an INSERT, for example
    #
    # https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html#preventing-implicit-io-when-using-asyncsession
    __mapper_args__ = {"eager_defaults": True}

    @declared_attr
    def __tablename__(cls):
        """
        By default, turn the model's camel-case class name
        into a snake-case table name. Override by providing
        an explicit `__tablename__` class property.
        """
        return camel_to_snake.sub("_", cls.__name__).lower()


naming_convention = {
    "ix": "ix_%(table_name)s__%(column_0_N_name)s",
    "uq": "uq_%(table_name)s__%(column_0_N_name)s",
    "ck": "ck_%(table_name)s__%(constraint_name)s",
    "fk": "fk_%(table_name)s__%(column_0_N_name)s__%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

Base = declarative_base(
    cls=ORMModel, metadata=MetaData(naming_convention=naming_convention)
)

Model = TypeVar("Model", bound=ORMModel)
