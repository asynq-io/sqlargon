import uuid
from typing import Any

from sqlalchemy import CHAR, UUID, Dialect, FunctionElement
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.type_api import TypeDecorator
from sqlalchemy.types import TypeEngine


class GUID(TypeDecorator):
    """
    Platform-independent UUID type.

    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(36), storing as stringified hex values with
    hyphens.
    """

    impl = UUID
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(postgresql.UUID(as_uuid=True))
        return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value: Any, dialect: Dialect) -> Any:
        if value is None:
            return None
        if dialect.name == "postgresql":
            if isinstance(value, str):
                value = uuid.UUID(value)
            return value
        if isinstance(value, uuid.UUID):
            return str(value)
        return str(uuid.UUID(value))

    def process_result_value(self, value: Any, dialect: Dialect) -> uuid.UUID | None:
        if value is None:
            return value
        if not isinstance(value, uuid.UUID):
            value = uuid.UUID(value)
        return value


class GenerateUUID(FunctionElement):
    name = "uuid_default"


@compiles(GenerateUUID, "postgresql")
@compiles(GenerateUUID)
def _generate_uuid_postgresql(
    _element: GenerateUUID, _compiler: Any, **_kwargs: Any
) -> str:
    """
    Generates a UUID in Postgres. Uses uuidv7() on PostgreSQL 18+,
    otherwise falls back to GEN_RANDOM_UUID() which requires pgcrypto.
    """
    version_info = getattr(_compiler.dialect, "server_version_info", None)
    if version_info and version_info >= (18,):
        return "uuidv7()"
    return "GEN_RANDOM_UUID()"


@compiles(GenerateUUID, "sqlite")
def _generate_uuid_sqlite(
    _element: GenerateUUID, _compiler: Any, **_kwargs: Any
) -> str:
    """
    Generates a random UUID in other databases (SQLite) by concatenating
    bytes in a way that approximates a UUID hex representation. This is
    sufficient for our purposes of having a random client-generated ID
    that is compatible with a UUID spec.
    """

    return """
    (
        lower(hex(randomblob(4)))
        || '-'
        || lower(hex(randomblob(2)))
        || '-4'
        || substr(lower(hex(randomblob(2))),2)
        || '-'
        || substr('89ab',abs(random()) % 4 + 1, 1)
        || substr(lower(hex(randomblob(2))),2)
        || '-'
        || lower(hex(randomblob(6)))
    )
    """
