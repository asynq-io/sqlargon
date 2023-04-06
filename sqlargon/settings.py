from pydantic import BaseSettings, Field, PyObject


class DatabaseSettings(BaseSettings):
    url: str = Field("postgresql+asyncpg://localhost:5432", env="DATABASE_URL")
    pool_size: int = Field(10, env="DATABASE_POOL_SIZE")
    echo: bool = Field(False, env="DATABASE_ECHO")
    echo_pool: bool = Field(True, env="DATABASE_ECHO_POOL")
    max_overflow: int = Field(0, env="DATABASE_MAX_OVERFLOW")
    pool_recycle: int = Field(3600, env="DATABASE_POOL_RECYCLE")
    poolclass: PyObject = Field(
        "sqlalchemy.AsyncAdaptedQueuePool", env="DATABASE_POOL_CLASS"
    )
    json_serializer: PyObject = Field(
        "sqlargon.util.json_dumps", env="DATABASE_JSON_SERIALIZER"
    )
    json_deserializer: PyObject = Field(
        "sqlargon.util.json_loads", env="DATABASE_JSON_DESERIALIZER"
    )
