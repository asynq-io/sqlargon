import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio
import sqlalchemy as sa

from sqlargon import Database, SQLAlchemyRepository
from sqlargon.types import GUID, GenerateUUID


@pytest_asyncio.fixture(scope="session")
def event_loop():
    return asyncio.new_event_loop()


@pytest_asyncio.fixture(scope="session")
def db():
    return Database.from_env()


@pytest_asyncio.fixture(scope="session")
def user_model(db: Database):
    class User(db.Model):  # type: ignore[name-defined]
        __tablename__ = "user"
        id = sa.Column(
            GUID(), primary_key=True, server_default=GenerateUUID(), nullable=False
        )
        name = sa.Column(sa.Unicode(255))
        last_name = sa.Column(sa.Unicode(255), nullable=True)

    yield User


@pytest_asyncio.fixture()
async def user_repository_class(user_model, db):
    class UserRepository(SQLAlchemyRepository[user_model]):
        default_order_by = user_model.name.desc()

        @property
        def on_conflict(self):
            return {"set_": {"name"}, "index_elements": ["id"]}

    await db.create_all()
    yield UserRepository
    await db.drop_all()


@pytest_asyncio.fixture()
async def user_repository(user_repository_class, db):
    yield user_repository_class(db)


@pytest.fixture
def user_data():
    return {"id": uuid4(), "name": "John"}
