from sqlaurum import Database


def test_create_database():
    db = Database(url="sqlite+aiosqlite:///:memory:")
    assert isinstance(db, Database)
