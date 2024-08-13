from uuid import uuid4

import pytest
import sqlalchemy as sa

from sqlargon import Base, Database, SQLAlchemyRepository, SQLAlchemyUnitOfWork
from sqlargon.mixins import UUIDModelMixin
from sqlargon.repository import OnConflict


class User(Base, UUIDModelMixin):  # type: ignore[name-defined]
    __tablename__ = "user"
    # id = sa.Column(
    #     GUID(), primary_key=True, nullable=False
    # )
    name = sa.Column(sa.Unicode(255))
    last_name = sa.Column(sa.Unicode(255), nullable=True)


class UserRepository(SQLAlchemyRepository[User]):
    default_order_by = User.name.desc()

    @property
    def on_conflict(self) -> OnConflict:
        return {"set_": {"name"}, "index_elements": ["name"]}


@pytest.fixture(scope="session", autouse=True)
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def db():
    db_ = Database(url="sqlite+aiosqlite:///:memory:", force_rollback=True)
    await db_.connect()
    await db_.create_all()
    yield db_
    await db_.drop_all()
    await db_.disconnect()
    # return Database.from_env()


@pytest.fixture
def user_repository(db):
    return UserRepository(db)


@pytest.fixture
def user_uow(db):
    class TestUow(SQLAlchemyUnitOfWork):
        users: UserRepository

    return TestUow(db)


@pytest.fixture
def user_data():
    return {"id": uuid4(), "name": "John"}
