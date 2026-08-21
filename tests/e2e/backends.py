"""The real databases the e2e suite runs against.

PostgreSQL, MySQL and MariaDB come from throwaway ``testcontainers``
containers, SQLite from a file on disk -- so no backend here is the in-memory
database the unit suite uses. Images are pinned but overridable, e.g.
``SQLARGON_E2E_POSTGRES_IMAGE=postgres:16-alpine``.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from contextlib import AbstractContextManager
    from pathlib import Path

# ``uuidv7()`` is a PostgreSQL 18 builtin, so GenerateUUIDV7 needs at least it.
# The pgvector image is that PostgreSQL plus the extension the vector suite
# needs, so it stands in for the plain one rather than adding a backend.
POSTGRES_IMAGE = os.environ.get("SQLARGON_E2E_POSTGRES_IMAGE", "pgvector/pgvector:pg18")
MYSQL_IMAGE = os.environ.get("SQLARGON_E2E_MYSQL_IMAGE", "mysql:8.4")
# RANDOM_BYTES, which the UUID server defaults use, needs MariaDB 10.10
MARIADB_IMAGE = os.environ.get("SQLARGON_E2E_MARIADB_IMAGE", "mariadb:11.4")


@contextmanager
def _postgres_url(_directory: Path) -> Generator[str]:
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as container:
        yield container.get_connection_url()


@contextmanager
def _mysql_url(_directory: Path) -> Generator[str]:
    from testcontainers.community.mysql import MySqlContainer

    with MySqlContainer(MYSQL_IMAGE, dialect="asyncmy") as container:
        yield container.get_connection_url()


@contextmanager
def _mariadb_url(_directory: Path) -> Generator[str]:
    from testcontainers.community.mysql import MySqlContainer
    from testcontainers.core.wait_strategies import ExecWaitStrategy

    class MariaDbContainer(MySqlContainer):
        """MariaDB, waited on with the healthcheck script of its own image.

        The entrypoint initialises the data directory with a temporary server
        that logs ``ready for connections`` before shutting down again, so the
        log message the MySQL container waits for arrives too early here.
        """

        def _connect(self) -> None:
            ExecWaitStrategy(
                ["healthcheck.sh", "--connect", "--innodb_initialized"]
            ).wait_until_ready(self)

    # the sqlargon dialect dispatch keys off the SQLAlchemy dialect name, and
    # only the "mysql" one reaches the MySQL query builder and column types --
    # see test_capabilities.test_mariadb_scheme_falls_back_to_the_base_builder
    with MariaDbContainer(MARIADB_IMAGE, dialect="asyncmy") as container:
        yield container.get_connection_url()


@contextmanager
def _sqlite_url(directory: Path) -> Generator[str]:
    yield f"sqlite+aiosqlite:///{directory / 'e2e.sqlite'}"


@dataclass(frozen=True, slots=True)
class Backend:
    """A database the e2e suite runs against, and what its server supports.

    ``dialect`` is the SQLAlchemy dialect name sqlargon sees, which is
    ``mysql`` for both the MySQL and the MariaDB backend; tell the two servers
    apart with :attr:`is_mariadb`.

    The capability flags record what the *server* accepts, which is not always
    what :class:`~sqlargon.query_builder.QueryBuilder` claims -- tests needing
    a capability the backend lacks are skipped, and the mismatch itself is
    asserted by a dedicated test.
    """

    name: str
    dialect: str
    driver: str
    url: Callable[[Path], AbstractContextManager[str]]
    insert_returning: bool
    update_returning: bool
    delete_returning: bool
    native_locks: bool
    server_side_uuid: bool
    skip_locked: bool
    json_key_operators: bool
    #: an upsert may leave a column of the conflict set out of its values
    partial_upsert: bool = True
    is_mariadb: bool = False
    #: the server can search vectors -- pgvector, or the sqlite-vector
    #: loadable extension
    vector_search: bool = False

    @property
    def is_mysql_family(self) -> bool:
        return self.dialect == "mysql"


BACKENDS: dict[str, Backend] = {
    "sqlite": Backend(
        name="sqlite",
        dialect="sqlite",
        driver="aiosqlite",
        url=_sqlite_url,
        insert_returning=True,
        update_returning=True,
        delete_returning=True,
        native_locks=False,
        server_side_uuid=True,
        skip_locked=False,
        # the SQLite key operators match JSON values, not object keys
        json_key_operators=False,
        vector_search=True,
    ),
    "postgres": Backend(
        name="postgres",
        dialect="postgresql",
        driver="asyncpg",
        url=_postgres_url,
        insert_returning=True,
        update_returning=True,
        delete_returning=True,
        native_locks=True,
        server_side_uuid=True,
        skip_locked=True,
        json_key_operators=True,
        vector_search=True,
    ),
    "mysql": Backend(
        name="mysql",
        dialect="mysql",
        driver="asyncmy",
        url=_mysql_url,
        # MySQL has no RETURNING clause at all, unlike MariaDB
        insert_returning=False,
        update_returning=False,
        delete_returning=False,
        native_locks=True,
        server_side_uuid=True,
        skip_locked=True,
        json_key_operators=True,
        # 8.0.19+ names the conflicting row with an alias exposing only the
        # columns the insert names, so an omitted one cannot be assigned
        partial_upsert=False,
    ),
    "mariadb": Backend(
        name="mariadb",
        dialect="mysql",
        driver="asyncmy",
        url=_mariadb_url,
        # INSERT since 10.5 and DELETE since 10.0.5, but never UPDATE
        insert_returning=True,
        update_returning=False,
        delete_returning=True,
        native_locks=True,
        server_side_uuid=True,
        skip_locked=True,
        json_key_operators=True,
        is_mariadb=True,
    ),
}


def parse_backends(names: str) -> list[Backend]:
    """Resolve a comma separated ``--e2e-backends`` value to backends."""
    selected = [name.strip() for name in names.split(",") if name.strip()]
    unknown = sorted(set(selected) - set(BACKENDS))
    if unknown:
        msg = f"Unknown e2e backends {unknown}; available: {sorted(BACKENDS)}"
        raise ValueError(msg)
    return [BACKENDS[name] for name in selected]
