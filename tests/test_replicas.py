import pytest
import sqlalchemy as sa

from sqlargon import (
    Database,
    DatabaseCluster,
    PrimaryReplicaRouter,
    ReadOnlyDatabase,
    ReadOnlyError,
    RoutingContext,
    SQLAlchemyRepository,
    read_only,
)
from tests import MEMORY_URL


def select_context() -> RoutingContext:
    return RoutingContext.create(sa.select(sa.literal(1)))


@pytest.fixture
def replicated_db():
    return DatabaseCluster.with_replicas(
        MEMORY_URL, read_replicas=[MEMORY_URL], auto_route=True
    )


def test_builds_read_replicas():
    cluster = DatabaseCluster.with_replicas(
        MEMORY_URL, read_replicas=[MEMORY_URL, MEMORY_URL]
    )
    assert len(cluster.replicas) == 2
    assert all(isinstance(replica, ReadOnlyDatabase) for replica in cluster.replicas)


def test_replica_from_mapping_overrides_engine_kwargs():
    cluster = DatabaseCluster.with_replicas(
        MEMORY_URL, read_replicas=[{"url": MEMORY_URL, "echo": True}]
    )
    assert len(cluster.replicas) == 1
    assert cluster.replicas[0].engine.echo is True
    assert cluster.default_database.engine.echo is not True


def test_cluster_from_env_with_replicas_builds_cluster():
    cluster = DatabaseCluster.from_env(url=MEMORY_URL, read_replicas=[MEMORY_URL])
    assert isinstance(cluster, DatabaseCluster)
    assert len(cluster.replicas) == 1


def test_route_select_with_auto_route(replicated_db):
    assert replicated_db.route(select_context()) in replicated_db.replicas


def test_route_select_without_auto_route():
    cluster = DatabaseCluster.with_replicas(MEMORY_URL, read_replicas=[MEMORY_URL])
    assert cluster.route(select_context()) is cluster.default_database


def test_route_write_not_routed(replicated_db, user_model):
    context = RoutingContext.create(sa.insert(user_model).values(name="John"))
    assert replicated_db.route(context) is replicated_db.default_database


def test_route_read_only_flag_forces_replica():
    cluster = DatabaseCluster.with_replicas(MEMORY_URL, read_replicas=[MEMORY_URL])
    assert cluster.route(RoutingContext.create(read_only=True)) in cluster.replicas


async def test_read_only_decorator_routes_to_replica():
    cluster = DatabaseCluster.with_replicas(MEMORY_URL, read_replicas=[MEMORY_URL])

    @read_only
    async def routed():
        return cluster.route(select_context())

    assert await routed() in cluster.replicas
    assert cluster.route(select_context()) is cluster.default_database


async def test_route_in_transaction_uses_primary(replicated_db):
    async with replicated_db.session_context():
        assert replicated_db.route(select_context()) is replicated_db.default_database


def test_sticky_reads_route_to_primary_after_write(replicated_db, user_model):
    write_context = RoutingContext.create(sa.insert(user_model).values(name="John"))
    assert replicated_db.route(write_context) is replicated_db.default_database
    assert replicated_db.route(select_context()) is replicated_db.default_database


def test_sticky_reads_disabled(user_model):
    primary = Database(MEMORY_URL)
    replica = ReadOnlyDatabase(MEMORY_URL)
    cluster = DatabaseCluster(
        {"primary": primary, "replica_0": replica},
        router=PrimaryReplicaRouter(replicas=["replica_0"], sticky_reads=False),
    )
    write_context = RoutingContext.create(sa.insert(user_model).values(name="John"))
    assert cluster.route(write_context) is primary
    assert cluster.route(select_context()) is replica


def test_round_robin_strategy_cycles_replicas():
    cluster = DatabaseCluster.with_replicas(
        MEMORY_URL,
        read_replicas=[MEMORY_URL, MEMORY_URL],
        auto_route=True,
        replica_strategy="round_robin",
    )
    first = cluster.route(select_context())
    second = cluster.route(select_context())
    third = cluster.route(select_context())
    assert first is not second
    assert first is third


async def test_read_only_database_allows_select():
    replica = ReadOnlyDatabase(MEMORY_URL)
    result = await replica.execute(sa.select(sa.literal(1)))
    assert result.scalar() == 1


async def test_read_only_database_rejects_write(user_model):
    replica = ReadOnlyDatabase(MEMORY_URL)
    with pytest.raises(ReadOnlyError):
        await replica.execute(sa.insert(user_model).values(name="John"))


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO t (x) VALUES (1)",
        "  update t set x = 1",
        "DELETE FROM t",
        "DROP TABLE t",
        "WITH ids AS (SELECT 1 AS x) INSERT INTO t SELECT x FROM ids",
    ],
)
async def test_read_only_database_rejects_text_writes(statement):
    replica = ReadOnlyDatabase(MEMORY_URL)
    with pytest.raises(ReadOnlyError):
        await replica.execute(sa.text(statement))


async def test_read_only_database_allows_text_select():
    replica = ReadOnlyDatabase(MEMORY_URL)
    result = await replica.execute(
        sa.text("WITH ids AS (SELECT 1 AS x) SELECT x FROM ids")
    )
    assert result.scalar() == 1


def test_read_only_database_sets_postgresql_readonly_option():
    pytest.importorskip("asyncpg")
    replica = ReadOnlyDatabase("postgresql+asyncpg://localhost:5432/test")
    options = replica.engine.sync_engine.get_execution_options()
    assert options["postgresql_readonly"] is True


async def test_dispose_disposes_replicas(replicated_db):
    await replicated_db.dispose()


async def test_repository_reads_route_through_replica(tmp_path, user_model, user_data):
    url = f"sqlite+aiosqlite:///{tmp_path / 'db.sqlite'}"
    cluster = DatabaseCluster.with_replicas(url, read_replicas=[url], auto_route=True)

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    await cluster.create_all()
    try:
        repository = Repository().using(db=cluster)
        await repository.create(**user_data)
        users = await repository.all()
        assert len(users) == 1
        assert users[0].name == user_data["name"]
    finally:
        await cluster.drop_all()
        await cluster.dispose()


async def test_read_only_database_allows_cte_with_write_verb_in_literal():
    replica = ReadOnlyDatabase(MEMORY_URL)
    result = await replica.execute(
        sa.text(
            "WITH recent AS (SELECT 'delete' AS action) "
            "SELECT count(*) FROM recent WHERE action = 'delete'"
        )
    )
    assert result.scalar() == 1


@pytest.mark.parametrize(
    "statement",
    [
        "-- audit\nINSERT INTO t (x) VALUES (1)",
        "/* hint */ UPDATE t SET x = 1",
        "SELECT 1; DELETE FROM t",
        "CALL write_proc()",
    ],
)
async def test_read_only_database_rejects_obfuscated_text_writes(statement):
    replica = ReadOnlyDatabase(MEMORY_URL)
    with pytest.raises(ReadOnlyError):
        await replica.execute(sa.text(statement))


async def test_read_only_sqlite_connection_is_query_only():
    replica = ReadOnlyDatabase(MEMORY_URL)
    result = await replica.execute(sa.text("PRAGMA query_only"))
    assert result.scalar() == 1
