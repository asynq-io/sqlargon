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
        if self.poolclass in {NullPool, StaticPool}:
            return {"poolclass": self.poolclass}
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
    connect_args: Optional[Dict[str, Any]] = Field(None, env="DATABASE_CONNECT_ARGS")
    pool_settings: PoolSettings = Field(default_factory=PoolSettings)

    def to_kwargs(self) -> Dict[str, Any]:
        kwargs = self.dict(exclude={"pool_settings"}, exclude_none=True)
        kwargs.update(self.pool_settings.to_kwargs())
        return kwargs

    class Config:
        extra = Extra.allow
        arbitrary_types_allowed = True
