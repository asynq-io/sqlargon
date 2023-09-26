from uuid import uuid4

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base

from sqlargon import Database, SQLAlchemyRepository
from sqlargon.types import GUID, GenerateUUID


@pytest.fixture(scope="session")
def db():
    return Database.from_env()


@pytest_asyncio.fixture(scope="function")
async def session(db: Database):
    async with db.session() as db_session:
        yield db_session


@pytest_asyncio.fixture(scope="function")
async def user_model(db):

    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"
        id = sa.Column(
            GUID(), primary_key=True, server_default=GenerateUUID(), nullable=False
        )
        name = sa.Column(sa.Unicode(255))
        last_name = sa.Column(sa.Unicode(255), nullable=True)

    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        yield User
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture()
def user_repository_class(user_model):
    class UserRepository(SQLAlchemyRepository[user_model]):
        @property
        def on_conflict(self):
            return {"set_": {"name"}, "index_elements": ["id"]}

    return UserRepository


@pytest_asyncio.fixture()
async def user_repository(user_repository_class, session):
    yield user_repository_class(session)


@pytest.fixture
def user_data():
    return {"id": uuid4(), "name": "John"}
