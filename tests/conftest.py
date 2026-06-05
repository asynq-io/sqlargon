from uuid import uuid4

import pytest
import sqlalchemy as sa

from sqlargon import Database, SQLAlchemyRepository
from sqlargon.types import GUID, GenerateUUID
from sqlargon.typing import OnConflictOptions


@pytest.fixture(scope="session", autouse=True)
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

    return User


@pytest.fixture
async def user_repository_class(user_model, db):
    class UserRepository(SQLAlchemyRepository[user_model]):
        default_order_by = user_model.name.desc()

        @property
        def on_conflict(self) -> OnConflictOptions:
            return {
                "set_": {"name"},
                "index_elements": {"id"},
                "exclude_set": {"last_name"},
            }

    await db.create_all()
    yield UserRepository
    await db.drop_all()


@pytest.fixture
async def user_repository(user_repository_class):
    return user_repository_class()


@pytest.fixture
def user_data():
    return {"id": uuid4(), "name": "John"}
