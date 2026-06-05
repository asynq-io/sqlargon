from collections.abc import Callable
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Pool

from .imports import ImportedType


class DatabaseSettings(BaseSettings):
    url: str = "postgresql+asyncpg://localhost:5432"
    name: str = "default"
    echo: bool = False
    isolation_level: str | None = None
    json_serializer: ImportedType[Callable[[Any], str]] | None = None
    json_deserializer: ImportedType[Callable[[str], Any]] | None = None
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
        return self.model_dump(exclude_none=True)
