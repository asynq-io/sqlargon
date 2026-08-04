import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from sqlargon import (
    Database,
    DatabaseCluster,
    ShardRouter,
    SQLAlchemyRepository,
    get_default_database,
    set_default_database,
)
from tests import MEMORY_URL


@pytest.fixture(autouse=True)
def reset_default_database():
    set_default_database(None)
    yield
    set_default_database(None)


def test_get_default_database_builds_from_env():
    default = get_default_database()
    assert isinstance(default, Database)
    assert default.dialect == "sqlite"
    assert get_default_database() is default


async def test_get_default_database_builds_cluster_when_replicas_configured(
    monkeypatch,
):
    monkeypatch.setenv("DATABASE_READ_REPLICAS", f'["{MEMORY_URL}"]')
    default = get_default_database()
    try:
        assert isinstance(default, DatabaseCluster)
        assert len(default.replicas) == 1
        assert get_default_database() is default
    finally:
        await default.dispose()


def test_set_default_database_overrides():
    db = Database(MEMORY_URL)
    set_default_database(db)
    assert get_default_database() is db


def test_set_default_database_returns_previous():
    first = Database(MEMORY_URL)
    second = Database(MEMORY_URL)
    assert set_default_database(first) is None
    assert set_default_database(second) is first
    assert set_default_database(None) is second


def test_get_default_database_builds_once_across_threads(monkeypatch):
    calls = []
    build = Database.from_env.__func__

    def slow_build(cls, **kwargs):
        calls.append(1)
        time.sleep(0.05)
        return build(cls, **kwargs)

    monkeypatch.setattr(Database, "from_env", classmethod(slow_build))
    with ThreadPoolExecutor(max_workers=4) as executor:
        databases = list(executor.map(lambda _: get_default_database(), range(4)))

    assert calls == [1]
    assert all(database is databases[0] for database in databases)


async def test_repository_without_database_uses_default(db, user_model, user_data):
    set_default_database(db)

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    assert Repository().db is db
    await db.create_all()
    try:
        user = await Repository().create(**user_data)
        assert user is not None
        assert await Repository().count() == 1
    finally:
        await db.drop_all()


async def test_repository_using_db_wins_over_default(db, user_model):
    set_default_database(db)
    other = Database(MEMORY_URL)

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    assert Repository().using(db=other).db is other


def test_cluster_from_env_single_primary():
    cluster = DatabaseCluster.from_env(url=MEMORY_URL)
    assert isinstance(cluster, DatabaseCluster)
    assert list(cluster.databases) == ["primary"]
    assert cluster.replicas == []


def test_cluster_from_env_with_replicas():
    cluster = DatabaseCluster.from_env(
        url=MEMORY_URL,
        read_replicas=[MEMORY_URL],
        auto_route=True,
        replica_strategy="round_robin",
    )
    assert len(cluster.replicas) == 1
    assert cluster.router.auto_route is True
    assert cluster.router.strategy == "round_robin"


def test_cluster_from_env_with_router_override():
    router = ShardRouter({0: "primary"}, default="primary")
    cluster = DatabaseCluster.from_env(url=MEMORY_URL, router=router)
    assert cluster.router is router
