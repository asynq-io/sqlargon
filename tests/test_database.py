from sqlargon import Database


def test_create_database(db: Database):
    assert isinstance(db, Database)


def test_database_supports(db: Database):
    assert db.supports_returning
    assert db.supports_on_conflict


def test_sqlite_version():
    import sqlite3

    assert sqlite3.sqlite_version > "3.35"


async def test_locks(db: Database):
    async with db.acquire_lock("test"):
        assert 1 == 1
