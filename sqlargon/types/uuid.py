import uuid

from sqlalchemy import CHAR, UUID, FunctionElement
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.type_api import TypeDecorator


class GUID(TypeDecorator):
    """
    Platform-independent UUID type.

    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(36), storing as stringified hex values with
    hyphens.
    """

    impl = UUID
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(postgresql.UUID(as_uuid=True))
        else:
            return dialect.type_descriptor(CHAR(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        elif dialect.name == "postgresql":
            if isinstance(value, str):
                value = uuid.UUID(value)
            return value
        elif isinstance(value, uuid.UUID):
            return str(value)
        else:
            return str(uuid.UUID(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value


class GenerateUUID(FunctionElement):
    name = "uuid_default"


@compiles(GenerateUUID, "postgresql")
@compiles(GenerateUUID)
def _generate_uuid_postgresql(element, compiler, **kwargs) -> str:
    """
    Generates a random UUID in Postgres; requires the pgcrypto extension.
    """

    return "(GEN_RANDOM_UUID())"


@compiles(GenerateUUID, "sqlite")
def _generate_uuid_sqlite(element, compiler, **kwargs) -> str:
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
