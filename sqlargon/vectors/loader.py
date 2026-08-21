from __future__ import annotations

from inspect import isawaitable
from typing import TYPE_CHECKING, Any
from weakref import WeakSet

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.util import await_only

from sqlargon.types.vector import UnsupportedDialectError

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine

    from sqlargon.database import Database

_registered_engines: WeakSet[Engine] = WeakSet()


async def init_vectors(db: Database) -> None:
    """Prepare ``db`` for vector search; call before ``create_all()``.

    On PostgreSQL this creates the ``vector`` extension -- applications
    managing their schema with alembic run
    ``op.execute("CREATE EXTENSION IF NOT EXISTS vector")`` in a migration
    instead. On SQLite it registers the sqlite-vector loadable extension
    on every new pool connection.
    """
    if db.dialect == "postgresql":
        await db.execute(sa.text("CREATE EXTENSION IF NOT EXISTS vector"))
    elif db.dialect == "sqlite":
        register_sqlite_vector(db.engine)
    else:
        msg = f"vector search is not supported on the {db.dialect!r} dialect"
        raise UnsupportedDialectError(msg)


def register_sqlite_vector(engine: AsyncEngine) -> None:
    """Load the sqlite-vector extension on every new pool connection.

    Register at startup, before queries run -- connections checked out
    earlier never get the extension. Registering the same engine twice
    is a no-op.
    """
    try:
        import importlib.resources

        import sqlite_vector  # noqa: F401
    except ImportError as e:
        msg = (
            "SQLite vector search requires the 'sqliteai-vector' package; "
            "install 'sqlargon[vectors-sqlite]'"
        )
        raise ImportError(msg) from e

    if engine.sync_engine in _registered_engines:
        return

    # SQLite appends the platform's shared library suffix itself
    path = str(importlib.resources.files("sqlite_vector.binaries") / "vector")

    def _resolve(result: Any) -> None:
        """Await what an async driver returns, pass a sync one's through.

        An async driver keeps its ``sqlite3`` object in a worker thread of
        its own and rejects calls from any other, so loading has to go
        through the coroutines it marshals -- awaited here on the greenlet
        the ``connect`` event already runs in.
        """
        if isawaitable(result):
            await_only(result)

    def _load_extension(dbapi_connection: Any, _record: Any) -> None:
        connection = getattr(dbapi_connection, "driver_connection", dbapi_connection)
        _resolve(connection.enable_load_extension(True))  # noqa: FBT003
        try:
            _resolve(connection.load_extension(path))
        finally:
            _resolve(connection.enable_load_extension(False))  # noqa: FBT003

    event.listen(engine.sync_engine, "connect", _load_extension)
    _registered_engines.add(engine.sync_engine)
