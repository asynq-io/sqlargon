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


class GenerateUUIDV7(FunctionElement):
    name = "uuidv7_default"


@compiles(GenerateUUID, "postgresql")
@compiles(GenerateUUID)
def _generate_uuid_postgresql(
    _element: GenerateUUID, _compiler: Any, **_kwargs: Any
) -> str:
    return "GEN_RANDOM_UUID()"


@compiles(GenerateUUIDV7, "postgresql")
@compiles(GenerateUUIDV7)
def _generate_uuidv7_postgresql(
    _element: GenerateUUID, _compiler: Any, **_kwargs: Any
) -> str:
    return "uuidv7()"


@compiles(GenerateUUID, "mysql")
def _generate_uuid_mysql(_element: GenerateUUID, _compiler: Any, **_kwargs: Any) -> str:
    """
    Generates a random UUID v4 in MySQL.

    Wrapped in parentheses, since MySQL only accepts an expression as a column
    default in the ``DEFAULT (expr)`` form, and using the ``MOD`` operator
    rather than ``%``, which the pyformat paramstyle of the MySQL drivers
    would read as a parameter placeholder.
    """
    return (
        "(LOWER(CONCAT("
        "HEX(RANDOM_BYTES(4)), '-',"
        "HEX(RANDOM_BYTES(2)), '-4',"
        "SUBSTR(HEX(RANDOM_BYTES(2)), 2), '-',"
        "ELT((CONV(SUBSTR(HEX(RANDOM_BYTES(1)), 2, 1), 16, 10) MOD 4) + 1, '8', '9', 'a', 'b'),"
        "SUBSTR(HEX(RANDOM_BYTES(2)), 2), '-',"
        "HEX(RANDOM_BYTES(6))"
        ")))"
    )


@compiles(GenerateUUIDV7, "mysql")
def _generate_uuidv7_mysql(
    _element: GenerateUUIDV7, _compiler: Any, **_kwargs: Any
) -> str:
    """
    Generates a UUID v7 in MySQL using the unix timestamp in milliseconds as the
    time component, and cryptographically random bytes for the random component.
    Requires MySQL 5.6.1+ (RANDOM_BYTES) and MySQL 5.6.4+ (NOW(3) precision),
    and MySQL 8.0.13+ to use it as a column default.

    Parenthesised and using ``MOD`` for the same reasons as
    :func:`_generate_uuid_mysql`.
    """
    return (
        "(LOWER(CONCAT("
        "LPAD(HEX(FLOOR(UNIX_TIMESTAMP(NOW(3)) * 1000) DIV 65536), 8, '0'), '-',"
        "LPAD(HEX(FLOOR(UNIX_TIMESTAMP(NOW(3)) * 1000) MOD 65536), 4, '0'), '-7',"
        "SUBSTR(HEX(RANDOM_BYTES(2)), 2), '-',"
        "ELT((CONV(SUBSTR(HEX(RANDOM_BYTES(1)), 2, 1), 16, 10) MOD 4) + 1, '8', '9', 'a', 'b'),"
        "SUBSTR(HEX(RANDOM_BYTES(2)), 2), '-',"
        "HEX(RANDOM_BYTES(6))"
        ")))"
    )


@compiles(GenerateUUID, "sqlite")
@compiles(GenerateUUIDV7, "sqlite")
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
