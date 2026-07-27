import pytest

from sqlargon import (
    Database,
    DatabaseCluster,
    ShardRouter,
    SQLAlchemyRepository,
    get_default_database,
    set_default_database,
)

MEMORY_URL = "sqlite+aiosqlite:///:memory:"


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


def test_set_default_database_overrides():
    db = Database(MEMORY_URL)
    set_default_database(db)
    assert get_default_database() is db


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
