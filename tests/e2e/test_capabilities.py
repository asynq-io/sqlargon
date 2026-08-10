"""What each server actually supports, next to what the query builder claims.

Where the two disagree the mismatch is asserted rather than skipped, so a
change on either side shows up as a failing test.
"""

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from sqlargon import Database
from sqlargon.query_builder import (
    Option,
    QueryBuilder,
    UnsupportedOption,
    get_query_builder,
)

from .backends import Backend
from .models import TABLES, User, UserRepository


def test_conflicts_are_supported_everywhere(db: Database):
    assert db.query_builder.supports(Option.CONFLICTS)


def test_locks_claim_matches_the_backend(db: Database, backend: Backend):
    assert db.query_builder.supports(Option.LOCKS) is backend.native_locks


def test_returning_claim_matches_the_backend(db: Database, backend: Backend):
    """The claim is made only where every statement can carry the clause.

    MariaDB returns rows from an INSERT and a DELETE but never from an
    UPDATE, and it reaches sqlargon as the same ``mysql`` dialect as MySQL,
    which has no RETURNING at all -- so the MySQL family claims none and the
    repository re-reads what it wrote instead.
    """
    every_statement = (
        backend.insert_returning
        and backend.update_returning
        and backend.delete_returning
    )
    assert db.query_builder.supports(Option.RETURNING) is every_statement
    assert every_statement is not backend.is_mysql_family


async def test_insert_returning_against_the_server(db: Database, backend: Backend):
    """A bare INSERT ... RETURNING, bypassing the repository fallback."""
    statement = sa.insert(User).values(name="John").returning(User)
    if backend.insert_returning:
        assert (await db.execute(statement)).scalars().one() is not None
    else:
        with pytest.raises(Exception, match=r"(?i)returning|syntax"):
            await db.execute(statement)


async def test_delete_returning_against_the_server(
    db: Database, users: UserRepository, backend: Backend
):
    """A bare DELETE ... RETURNING, bypassing the repository fallback."""
    await users.bulk_create([{"name": "John"}], return_results=False)
    statement = sa.delete(User).where(User.name == "John").returning(User)
    if backend.delete_returning:
        assert (await db.execute(statement)).scalars().one() is not None
    else:
        with pytest.raises(Exception, match=r"(?i)returning|syntax"):
            await db.execute(statement)


async def test_update_returning_against_the_server(
    db: Database, users: UserRepository, backend: Backend
):
    """A bare UPDATE ... RETURNING, which no MySQL family server accepts."""
    await users.bulk_create([{"name": "John"}], return_results=False)
    statement = (
        sa.update(User).where(User.name == "John").values(score=4).returning(User)
    )
    if backend.update_returning:
        assert (await db.execute(statement)).scalars().one() is not None
    else:
        with pytest.raises(Exception, match=r"(?i)returning|syntax"):
            await db.execute(statement)


def test_excluded_row_reference(db: Database, backend: Backend):
    """``excluded`` is unimplemented for MySQL, which names it ``inserted``."""
    if backend.is_mysql_family:
        with pytest.raises(UnsupportedOption):
            db.query_builder.excluded(User)
    else:
        assert db.query_builder.excluded(User) is not None


async def test_upsert_of_a_partial_row(users: UserRepository, backend: Backend):
    """An upsert whose values leave out a column of the conflict set.

    The omitted column is still assigned, from the value the insert would have
    given it -- so a column with no default is silently set to NULL, wiping
    what was stored. MySQL 8.0.19+ is the one backend that refuses instead:
    it has no ``excluded``, and the row alias it uses in place of one exposes
    only the columns the insert names.
    """
    await users.bulk_create(
        [{"name": "John", "last_name": "Doe"}], return_results=False
    )
    user = await users.get(name="John")
    assert user is not None
    statement = users.upsert({"id": user.id, "name": "John", "score": 7})

    if not backend.partial_upsert:
        with pytest.raises(OperationalError, match="last_name"):
            await statement.execute()
        return

    await statement.execute()
    refreshed = await users.get(name="John")
    assert refreshed is not None
    assert refreshed.score == 7
    assert refreshed.last_name is None


async def test_skip_locked_is_accepted_by_the_server(
    db: Database, users: UserRepository, backend: Backend
):
    await users.bulk_create([{"name": "John"}], return_results=False)
    async with db.session_context():
        rows = await users.select(with_for_update={"skip_locked": True}).all()
    assert len(rows) == 1
    assert backend.skip_locked is (backend.dialect != "sqlite")


async def test_server_variant_matches_the_backend(db: Database, backend: Backend):
    """MySQL and MariaDB both reach sqlargon as the ``mysql`` dialect."""
    if not backend.is_mysql_family:
        pytest.skip(f"{backend.name} is not of the MySQL family")
    assert db.dialect == "mysql"
    async with db.engine.connect() as connection:
        version = (await connection.execute(sa.text("SELECT VERSION()"))).scalar_one()
    assert ("mariadb" in version.lower()) is backend.is_mariadb
    assert getattr(db.engine.dialect, "is_mariadb", None) is backend.is_mariadb


async def test_tables_use_the_innodb_storage_engine(db: Database, backend: Backend):
    """InnoDB is what the transaction and locking tests above assume."""
    if not backend.is_mysql_family:
        pytest.skip(f"{backend.name} has no pluggable storage engine")
    async with db.engine.connect() as connection:
        rows = (
            await connection.execute(
                sa.text(
                    "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE()"
                )
            )
        ).all()
    expected = {table.name for table in TABLES}
    assert expected <= {name for name, _ in rows}
    assert {engine for name, engine in rows if name in expected} == {"InnoDB"}


def test_mariadb_scheme_falls_back_to_the_base_builder():
    """A ``mariadb+driver://`` URL is not recognised as the MySQL family.

    The dispatch keys off the SQLAlchemy dialect name, which that scheme
    reports as ``mariadb``; the MariaDB backend therefore connects over
    ``mysql+asyncmy://``, the only scheme sqlargon supports for it.
    """
    builder = get_query_builder("mariadb")
    assert type(builder) is QueryBuilder
    assert not builder.supports(Option.CONFLICTS)
    assert not builder.supports(Option.RETURNING)
    assert not builder.supports(Option.LOCKS)
