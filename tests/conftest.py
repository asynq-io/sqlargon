from uuid import uuid4

import pytest
import sqlalchemy as sa

from sqlargon import Database, SQLAlchemyRepository
from sqlargon.repository import OnConflict
from sqlargon.types import GUID, GenerateUUID


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
def db():
    return Database.from_env()


@pytest.fixture(scope="session")
def user_model(db: Database):
    class User(db.Model):  # type: ignore[name-defined]
        __tablename__ = "user"
        id = sa.Column(
            GUID(), primary_key=True, server_default=GenerateUUID(), nullable=False
        )
        name = sa.Column(sa.Unicode(255))
        last_name = sa.Column(sa.Unicode(255), nullable=True)

    yield User


@pytest.fixture
async def user_repository_class(user_model, db):
    class UserRepository(SQLAlchemyRepository[user_model]):
        default_order_by = user_model.name.desc()

        @property
        def on_conflict(self) -> OnConflict:
            return {"set_": {"name"}, "index_elements": ["id"]}

    await db.create_all()
    yield UserRepository
    await db.drop_all()


@pytest.fixture
async def user_repository(user_repository_class, db):
    yield user_repository_class(db)


@pytest.fixture
def user_data():
    return {"id": uuid4(), "name": "John"}
