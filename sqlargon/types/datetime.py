from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Dialect, FunctionElement, TypeDecorator
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import TypeEngine


class Timestamp(TypeDecorator):
    """TypeDecorator that ensures that timestamps have a timezone.

    For SQLite, all timestamps are converted to UTC (since they are stored
    as naive timestamps without timezones) and recovered as UTC.
    """

    impl = sa.TIMESTAMP(timezone=True)
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[datetime]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(postgresql.TIMESTAMP(timezone=True))
        if dialect.name == "sqlite":
            return dialect.type_descriptor(sqlite.DATETIME())
        if dialect.name == "mysql":
            return dialect.type_descriptor(mysql.DATETIME(fsp=6))
        return dialect.type_descriptor(sa.TIMESTAMP(timezone=True))

    def process_bind_param(self, value: Any, dialect: Dialect) -> Any:
        if value is None:
            return None
        if value.tzinfo is None:
            msg = "Timestamps must have a timezone."
            raise ValueError(msg)
        if dialect.name in ("sqlite", "mysql"):
            return value.astimezone(timezone.utc)
        return value

    def process_result_value(self, value: Any, dialect: Dialect) -> Any:
        # retrieve timestamps in their native timezone (or UTC)
        if value is not None:
            return value.replace(tzinfo=timezone.utc)
        return None


class now(FunctionElement):
    """
    Platform-independent "now" generator.
    """

    type = Timestamp()
    name = "now"
    # see https://docs.sqlalchemy.org/en/14/core/compiler.html#enabling-caching-support-for-custom-constructs
    inherit_cache = True


@compiles(now, "mysql")
def _current_timestamp_mysql(_element: now, _compiler: Any, **_kwargs: Any) -> str:
    """Generates the current timestamp for MySQL with microsecond precision."""
    return "CURRENT_TIMESTAMP(6)"


@compiles(now, "sqlite")
def _current_timestamp_sqlite(_element: now, _compiler: Any, **_kwargs: Any) -> str:
    """
    Generates the current timestamp for SQLite
    """
    return "strftime('%Y-%m-%d %H:%M:%f000', 'now')"


@compiles(now)
def _current_timestamp_postgresql(_element: now, _compiler: Any, **_kwargs: Any) -> str:
    """
    Generates the current timestamp in standard SQL
    """
    return "CURRENT_TIMESTAMP"
