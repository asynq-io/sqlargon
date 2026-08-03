from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .cluster import AnyDatabase


_default_database: AnyDatabase | None = None
_default_database_lock = threading.Lock()


def set_default_database(database: AnyDatabase | None) -> AnyDatabase | None:
    """Set the process-wide default database (or clear it with ``None``).

    Repositories and units of work that are not explicitly bound to a
    database fall back to this one.

    Returns the previously set database (if any) so the caller can
    ``dispose()`` it once it is no longer used -- replacing the default does
    not close the old engine's connection pool.
    """
    global _default_database  # noqa: PLW0603
    with _default_database_lock:
        previous = _default_database
        _default_database = database
    return previous


def get_default_database() -> AnyDatabase:
    """Return the default database, building it from ``DATABASE_*`` settings
    on first use.

    The built default is a plain :class:`~sqlargon.Database`, or a
    primary/replica :class:`~sqlargon.DatabaseCluster` when
    ``DATABASE_READ_REPLICAS`` is configured. Building is guarded by a lock,
    so concurrent first calls share one instance instead of leaking engines.
    """
    global _default_database  # noqa: PLW0603
    if _default_database is not None:
        return _default_database
    with _default_database_lock:
        if _default_database is None:
            _default_database = _build_default_database()
        return _default_database


def _build_default_database() -> AnyDatabase:
    from .settings import DatabaseClusterSettings

    if DatabaseClusterSettings().read_replicas:
        from .cluster import DatabaseCluster

        return DatabaseCluster.from_env()

    from .database import Database

    return Database.from_env()
