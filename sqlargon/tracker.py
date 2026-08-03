import traceback

import sqlalchemy as sa
from sqlalchemy import AdaptedConnection
from sqlalchemy.event import listen
from sqlalchemy.pool import ConnectionPoolEntry

ConnDict = dict[AdaptedConnection, list[str]]


class ConnectionTracker:
    """A test utility which tracks the connections given out by a connection pool, to
    make it easy to see which connections are currently checked out and open."""

    def __init__(self) -> None:
        self.open_connections: ConnDict = {}
        self.left_field_closes: ConnDict = {}
        self.connects = 0
        self.closes = 0

    def track_pool(self, pool: sa.Engine) -> None:
        listen(pool, "connect", self.on_connect)
        listen(pool, "close", self.on_close)
        listen(pool, "close_detached", self.on_close_detached)

    def on_connect(
        self,
        adapted_connection: AdaptedConnection,
        _connection_record: ConnectionPoolEntry,
    ) -> None:
        self.open_connections[adapted_connection] = traceback.format_stack()
        self.connects += 1

    def on_close(
        self,
        adapted_connection: AdaptedConnection,
        _connection_record: ConnectionPoolEntry,
    ) -> None:
        try:
            del self.open_connections[adapted_connection]
        except KeyError:
            self.left_field_closes[adapted_connection] = traceback.format_stack()
        self.closes += 1

    def on_close_detached(
        self,
        adapted_connection: AdaptedConnection,
    ) -> None:
        try:
            del self.open_connections[adapted_connection]
        except KeyError:
            self.left_field_closes[adapted_connection] = traceback.format_stack()
        self.closes += 1

    def clear(self) -> None:
        self.open_connections.clear()
        self.left_field_closes.clear()
        self.connects = 0
        self.closes = 0


TRACKER: ConnectionTracker = ConnectionTracker()
