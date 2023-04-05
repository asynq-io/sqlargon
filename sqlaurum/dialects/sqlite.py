import sqlite3

from sqlalchemy.dialects.sqlite import insert


def configure_sqlite_dialect(cls) -> None:
    cls._insert = staticmethod(insert)
    cls.supports_returning = sqlite3.sqlite_version > "3.35"
    cls.supports_on_conflict = True
