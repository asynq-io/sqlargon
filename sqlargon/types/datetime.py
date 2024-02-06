from datetime import timezone

import sqlalchemy as sa
from sqlalchemy import FunctionElement, TypeDecorator
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.compiler import compiles


class Timestamp(TypeDecorator):
    """TypeDecorator that ensures that timestamps have a timezone.

    For SQLite, all timestamps are converted to UTC (since they are stored
    as naive timestamps without timezones) and recovered as UTC.
    """

    impl = sa.TIMESTAMP(timezone=True)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(postgresql.TIMESTAMP(timezone=True))
        elif dialect.name == "sqlite":
            return dialect.type_descriptor(sqlite.DATETIME())
        else:
            return dialect.type_descriptor(sa.TIMESTAMP(timezone=True))

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        else:
            if value.tzinfo is None:
                raise ValueError("Timestamps must have a timezone.")
            elif dialect.name == "sqlite":
                return value.replace(tzinfo=timezone.utc)
            else:
                return value

    def process_result_value(self, value, dialect):
        # retrieve timestamps in their native timezone (or UTC)
        if value is not None:
            return value.replace(tzinfo=timezone.utc)


class now(FunctionElement):
    """
    Platform-independent "now" generator.
    """

    type = Timestamp()
    name = "now"
    # see https://docs.sqlalchemy.org/en/14/core/compiler.html#enabling-caching-support-for-custom-constructs
    inherit_cache = True


@compiles(now, "sqlite")
def _current_timestamp_sqlite(element, compiler, **kwargs):
    """
    Generates the current timestamp for SQLite
    """
    return "strftime('%Y-%m-%d %H:%M:%f000', 'now')"


@compiles(now)
def _current_timestamp_postgresql(element, compiler, **kwargs):
    """
    Generates the current timestamp in standard SQL
    """
    return "CURRENT_TIMESTAMP"
