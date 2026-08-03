from collections.abc import Callable
from typing import Any, Literal

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Pool

from .imports import ImportedType
from .utils import json_dumps, json_loads


class DatabaseSettings(BaseSettings):
    url: str = "postgresql+asyncpg://localhost:5432"
    echo: bool = False
    isolation_level: str | None = None
    json_serializer: ImportedType[Callable[[Any], str]] | None = json_dumps
    json_deserializer: ImportedType[Callable[[str], Any]] | None = json_loads
    connect_args: dict[str, Any] | None = None
    enable_tracker: bool = False
    poolclass: ImportedType[type[Pool]] | None = None
    pool_size: int | None = None
    max_overflow: int | None = None
    echo_pool: bool | None = None
    pool_recycle: int | None = None
    pool_pre_ping: bool | None = None
    pool_timeout: int | None = None
    pool_use_lifo: bool | None = None

    model_config = SettingsConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
        validate_default=True,
        env_prefix="DATABASE_",
    )

    def to_kwargs(self) -> dict[str, Any]:
        """Keyword arguments for the matching constructor."""
        return self.model_dump(exclude_none=True)


class DatabaseClusterSettings(DatabaseSettings):
    read_replicas: list[str] | None = None
    auto_route: bool = True
    replica_strategy: Literal["random", "round_robin"] = "random"
