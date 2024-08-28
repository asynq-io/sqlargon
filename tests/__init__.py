import os

os.environ.update(
    {
        "ENV": "TEST",
        "DATABASE_URL": "sqlite+aiosqlite:///:memory:",
    }
)
