from importlib.metadata import version

from .cluster import AnyDatabase, DatabaseCluster
from .database import BaseDatabase, Database, ReadOnlyDatabase, ReadOnlyError
from .functools import atomic
from .orm import Base, Model, ORMModel, SoftDeleteBase, SoftDeleteModel
from .registry import get_default_database, set_default_database
from .repository import (
    DeletedRowExistsError,
    SoftDeleteRepository,
    SQLAlchemyRepository,
)
from .routing import (
    DefaultRouter,
    ModelRouter,
    PrimaryReplicaRouter,
    Router,
    RoutingContext,
    RoutingError,
    RoutingOptions,
    ShardRouter,
    read_only,
    use_context,
    using,
)
from .uow import AbstractUnitOfWork, SQLAlchemyUnitOfWork

__version__ = version(__name__)

__all__ = [
    "AbstractUnitOfWork",
    "AnyDatabase",
    "Base",
    "BaseDatabase",
    "Database",
    "DatabaseCluster",
    "DefaultRouter",
    "DeletedRowExistsError",
    "Model",
    "ModelRouter",
    "ORMModel",
    "PrimaryReplicaRouter",
    "ReadOnlyDatabase",
    "ReadOnlyError",
    "Router",
    "RoutingContext",
    "RoutingError",
    "RoutingOptions",
    "SQLAlchemyRepository",
    "SQLAlchemyUnitOfWork",
    "ShardRouter",
    "SoftDeleteBase",
    "SoftDeleteModel",
    "SoftDeleteRepository",
    "__version__",
    "atomic",
    "get_default_database",
    "read_only",
    "set_default_database",
    "use_context",
    "using",
]
