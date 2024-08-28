import traceback

import sqlalchemy as sa
from sqlalchemy import AdaptedConnection
from sqlalchemy.pool import ConnectionPoolEntry

ConnDict = dict[AdaptedConnection, list[str]]


class ConnectionTracker:
    """A test utility which tracks the connections given out by a connection pool, to
    make it easy to see which connections are currently checked out and open."""

    def __init__(self) -> None:
        self.all_connections: ConnDict = {}
        self.open_connections: ConnDict = {}
        self.left_field_closes: ConnDict = {}
        self.connects = 0
        self.closes = 0

    def track_pool(self, pool: sa.pool.Pool):
        sa.event.listen(pool, "connect", self.on_connect)
        sa.event.listen(pool, "close", self.on_close)
        sa.event.listen(pool, "close_detached", self.on_close_detached)

    def on_connect(
        self,
        adapted_connection: AdaptedConnection,
        connection_record: ConnectionPoolEntry,
    ):
        self.all_connections[adapted_connection] = traceback.format_stack()
        self.open_connections[adapted_connection] = traceback.format_stack()
        self.connects += 1

    def on_close(
        self,
        adapted_connection: AdaptedConnection,
        connection_record: ConnectionPoolEntry,
    ):
        try:
            del self.open_connections[adapted_connection]
        except KeyError:
            self.left_field_closes[adapted_connection] = traceback.format_stack()
        self.closes += 1

    def on_close_detached(
        self,
        adapted_connection: AdaptedConnection,
    ):
        try:
            del self.open_connections[adapted_connection]
        except KeyError:
            self.left_field_closes[adapted_connection] = traceback.format_stack()
        self.closes += 1

    def clear(self):
        self.all_connections.clear()
        self.open_connections.clear()
        self.left_field_closes.clear()
        self.connects = 0
        self.closes = 0


TRACKER: ConnectionTracker = ConnectionTracker()
