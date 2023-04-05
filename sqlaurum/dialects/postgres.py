from sqlalchemy.dialects.postgresql import insert


def configure_postgres_dialect(cls) -> None:
    cls._insert = staticmethod(insert)
    cls.supports_returning = True
    cls.supports_on_conflict = True
