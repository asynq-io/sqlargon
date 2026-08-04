import os

MEMORY_URL = "sqlite+aiosqlite:///:memory:"

os.environ.update(
    {
        "ENV": "TEST",
        "DATABASE_URL": MEMORY_URL,
    }
)
