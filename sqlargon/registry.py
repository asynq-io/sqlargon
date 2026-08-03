from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .cluster import AnyDatabase


_default_database: AnyDatabase | None = None


def set_default_database(database: AnyDatabase | None) -> None:
    """Set the process-wide default database (or clear it with ``None``).

    Repositories and units of work that are not explicitly bound to a
    database fall back to this one.
    """
    global _default_database  # noqa: PLW0603
    _default_database = database


def get_default_database() -> AnyDatabase:
    """Return the default database, building it from ``DATABASE_*`` settings
    on first use.

    The built default is a plain :class:`~sqlargon.Database`, or a
    primary/replica :class:`~sqlargon.DatabaseCluster` when
    ``DATABASE_READ_REPLICAS`` is configured.
    """
    global _default_database  # noqa: PLW0603
    if _default_database is None:
        from .settings import DatabaseClusterSettings

        if DatabaseClusterSettings().read_replicas:
            from .cluster import DatabaseCluster

            _default_database = DatabaseCluster.from_env()
        else:
            from .database import Database

            _default_database = Database.from_env()
    return _default_database
