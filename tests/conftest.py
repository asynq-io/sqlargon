from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from sqlargon import Database, SQLAlchemyRepository, set_default_database
from sqlargon.types import GUID, GenerateUUID
from sqlargon.typing import OnConflictOptions
from tests.e2e.backends import BACKENDS

E2E_DIR = Path(__file__).parent / "e2e"

# the first test of a backend pays for pulling and starting its container,
# which the per-test timeout of the unit suite is far too tight for
E2E_TIMEOUT = 300


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("sqlargon")
    group.addoption(
        "--e2e",
        action="store_true",
        default=False,
        help="Run the e2e suite against real databases instead of skipping it.",
    )
    group.addoption(
        "--e2e-backends",
        default=",".join(BACKENDS),
        metavar="NAMES",
        help=f"Comma separated e2e backends, out of {', '.join(BACKENDS)}.",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Mark the e2e tests, and skip them unless ``--e2e`` was given."""
    skip_e2e = pytest.mark.skip(reason="e2e suite disabled; run it with --e2e")
    for item in items:
        if E2E_DIR not in item.path.parents:
            continue
        item.add_marker("e2e")
        item.add_marker(pytest.mark.timeout(E2E_TIMEOUT))
        if not config.getoption("e2e"):
            item.add_marker(skip_e2e)


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Run the suite on asyncio only, the one loop SQLAlchemy supports."""
    return "asyncio"


@pytest.fixture(scope="session", autouse=True)
def db():
    return Database.from_env()


@pytest.fixture(autouse=True)
def default_database(db: Database):
    set_default_database(db)
    yield db
    set_default_database(None)


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
