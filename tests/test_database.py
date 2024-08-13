from sqlargon import Database


def test_create_database(db: Database):
    assert isinstance(db, Database)


def test_sqlite_version():
    import sqlite3

    assert sqlite3.sqlite_version > "3.35"


async def test_locks(db: Database):
    async with db.lock("test"):
        assert 1 == 1
