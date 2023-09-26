import os

os.environ.update(
    {
        "ENV": "TEST",
        "DATABASE_URL": "sqlite+aiosqlite:///:memory:",
        # "DATABASE_URL": "sqlite+aiosqlite:///tmp.db",
        "DATABASE_POOL_CLASS": "sqlalchemy.StaticPool",
    }
)
