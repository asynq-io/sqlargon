from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database import Database


class DatabaseRegistry:
    """Registry of named ``Database`` instances.

    Allows repositories, units of work, and FastAPI dependencies to resolve a
    ``Database`` by name at runtime — useful for read-replica / write-replica
    topologies.
    """

    def __init__(self) -> None:
        self._databases: dict[str, Database] = {}

    def register(self, database: Database) -> Database:
        if database.name in self._databases:
            msg = f"Database {database.name} already registered"
            raise ValueError(msg)
        self._databases[database.name] = database
        return database

    def unregister(self, name: str) -> Database | None:
        return self._databases.pop(name, None)

    def get(self, name: str = "default") -> Database:
        return self[name]

    def clear(self) -> None:
        self._databases.clear()

    def __contains__(self, name: str) -> bool:
        return name in self._databases

    def __getitem__(self, name: str) -> Database:
        return self._databases[name]


db_registry = DatabaseRegistry()
