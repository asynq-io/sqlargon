import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from sqlaurum.function_elements import GenerateUUID
from sqlaurum.types import GUID


@pytest.fixture(scope="session")
def engine():
    return create_async_engine("sqlite+aiosqlite:///:memory:")


@pytest_asyncio.fixture(scope="session")
def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest_asyncio.fixture(scope="function")
async def session(session_factory):
    async with session_factory() as db_session:
        try:
            yield db_session
            await db_session.commit()
        except:  # noqa
            await db_session.rollback()
            raise
        finally:
            await db_session.close()


@pytest_asyncio.fixture(scope="function")
async def user_model(engine):

    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"
        id = sa.Column(
            GUID(), primary_key=True, server_default=GenerateUUID(), nullable=False
        )
        name = sa.Column(sa.Unicode(255))

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        yield User
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture()
def user_repository_class(user_model, repo_cls):
    class UserRepository(repo_cls[user_model]):
        @property
        def on_conflict(self):
            return {"set_": {"name"}, "index_elements": ["id"]}

    return UserRepository


@pytest_asyncio.fixture()
async def user_repository(user_repository_class, session):
    yield user_repository_class(session)
