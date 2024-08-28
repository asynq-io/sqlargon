from typing import Any, Callable, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Pool

from .imports import ImportedType


class DatabaseSettings(BaseSettings):
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
    enable_tracker: bool = True
    poolclass: Optional[ImportedType[type[Pool]]] = None
    pool_size: Optional[int] = None
    max_overflow: Optional[int] = None
    echo_pool: Optional[bool] = None
    pool_recycle: Optional[int] = None
    pool_pre_ping: Optional[bool] = None
    pool_timeout: Optional[int] = None
    pool_use_lifo: Optional[bool] = None

    model_config = SettingsConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
        validate_default=True,
        env_prefix="DATABASE_",
    )

    def to_kwargs(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True)
