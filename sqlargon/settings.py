from abc import ABC, abstractmethod
from typing import Any, Callable, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import NullPool, Pool, StaticPool

from .imports import ImportedType


class AbstractDbSettings(BaseSettings, ABC):
    model_config = SettingsConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
        validate_default=True,
        env_prefix="DATABASE_",
    )

    @abstractmethod
    def to_kwargs(self) -> dict[str, Any]:
        raise NotImplementedError


class PoolSettings(AbstractDbSettings):
    poolclass: ImportedType[type[Pool]] = Field("sqlalchemy:AsyncAdaptedQueuePool")
    pool_size: int = 5
    max_overflow: int = 5
    echo_pool: bool = False
    pool_recycle: int = 360
    pool_pre_ping: bool = True

    def to_kwargs(self) -> dict[str, Any]:
        if self.poolclass in {NullPool, StaticPool}:
            return {"poolclass": self.poolclass}
        return self.model_dump()


class DatabaseSettings(AbstractDbSettings):
    url: str = "postgresql+asyncpg://localhost:5432"
    echo: bool = False
    isolation_level: Optional[str] = None
    json_serializer: ImportedType[Callable[[Any], str]] = Field(
        "sqlargon.utils:json_dumps"
    )
    json_deserializer: ImportedType[Callable[[str], Any]] = Field(
        "sqlargon.utils:json_loads"
    )
    connect_args: Optional[dict[str, Any]] = None
    pool_settings: PoolSettings = Field(default_factory=PoolSettings)

    def to_kwargs(self) -> dict[str, Any]:
        kwargs = self.model_dump(exclude={"pool_settings"}, exclude_none=True)
        kwargs.update(self.pool_settings.to_kwargs())
        return kwargs
