from typing import Optional

from anqa.core.settings import BaseSettings
from anqa.core.utils.imports import ImportedType
from pydantic import Field


class DatabaseSettings(BaseSettings):
    url: str = Field("postgres://localhost:5432", env="URL")
    pool_size: int = Field(10, env="POOL_SIZE")
    echo_pool: bool = Field(True, env="ECHO_POOL")
    max_overflow: int = Field(0, env="MAX_OVERFLOW")
    pool_recycle: int = Field(3600, env="POOL_RECYCLE")
    poolclass: ImportedType = Field(
        "sqlalchemy:AsyncAdaptedQueuePool", env="POOL_CLASS"
    )
    json_serializer: ImportedType = Field(
        "anqa.core.utils.json:json_dumps", env="JSON_SERIALIZER"
    )
    json_deserializer: ImportedType = Field(
        "anqa.core.utils.json:json_loads", env="JSON_DESERIALIZER"
    )

    class Config:
        env_prefix = "DATABASE_"


class DatabaseSettingsMixin:
    database: Optional[DatabaseSettings] = Field(default_factory=DatabaseSettings)
