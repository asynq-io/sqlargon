from typing import Any, Dict, Optional

from pydantic import BaseSettings, Extra, Field, PyObject
from sqlalchemy import NullPool, StaticPool


class PoolSettings(BaseSettings):
    poolclass: PyObject = Field(
        "sqlalchemy.AsyncAdaptedQueuePool", env="DATABASE_POOL_CLASS"
    )
    pool_size: int = Field(10, env="DATABASE_POOL_SIZE")
    max_overflow: int = Field(5, env="DATABASE_MAX_OVERFLOW")
    echo_pool: bool = Field(False, env="DATABASE_ECHO_POOL")
    pool_recycle: int = Field(3600, env="DATABASE_POOL_RECYCLE")
    pool_pre_ping: bool = Field(True, env="DATABASE_POOL_PRE_PING")

    def to_kwargs(self) -> Dict[str, Any]:
        if self.poolclass == NullPool:
            return {"poolclass": NullPool}
        elif self.poolclass == StaticPool:
            return {
                "poolclass": StaticPool,
                "connect_args": {"check_same_thread": False},
            }
        return self.dict()


class DatabaseSettings(BaseSettings):
    url: str = Field("postgresql+asyncpg://localhost:5432", env="DATABASE_URL")
    echo: bool = Field(False, env="DATABASE_ECHO")
    isolation_level: Optional[str] = Field(None, env="DATABASE_ISOLATION_LEVEL")
    json_serializer: PyObject = Field(
        "sqlargon.util.json_dumps", env="DATABASE_JSON_SERIALIZER"
    )
    json_deserializer: PyObject = Field(
        "sqlargon.util.json_loads", env="DATABASE_JSON_DESERIALIZER"
    )
    pool_settings: PoolSettings = Field(default_factory=PoolSettings)

    def to_kwargs(self) -> Dict[str, Any]:
        kwargs = self.dict(exclude={"pool_settings"})
        kwargs.update(self.pool_settings.to_kwargs())
        return kwargs

    class Config:
        extra = Extra.allow
        arbitrary_types_allowed = True
