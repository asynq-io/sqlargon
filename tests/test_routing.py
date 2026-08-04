from uuid import uuid4

import pytest
import sqlalchemy as sa

from sqlargon import (
    Base,
    Database,
    DatabaseCluster,
    DefaultRouter,
    ModelRouter,
    RoutingContext,
    RoutingError,
    RoutingOptions,
    ShardRouter,
    SQLAlchemyRepository,
    SQLAlchemyUnitOfWork,
    atomic,
    use_context,
    using,
)
from sqlargon.types import GUID, GenerateUUID
from tests import MEMORY_URL

table = sa.table("t", sa.column("x"))


class RoutedItem(Base):
    __tablename__ = "test_routing_item"
    id = sa.Column(
        GUID(), primary_key=True, server_default=GenerateUUID(), nullable=False
    )


@pytest.mark.parametrize(
    ("statement", "operation"),
    [
        (sa.select(sa.literal(1)), "read"),
        (sa.insert(table).values(x=1), "write"),
        (sa.update(table).values(x=1), "write"),
        (sa.delete(table), "write"),
        (sa.text("SELECT 1"), "write"),
        (None, "write"),
    ],
)
def test_routing_context_operation_derivation(statement, operation):
    assert RoutingContext.create(statement).operation == operation


def test_routing_context_read_only_forces_read():
    context = RoutingContext.create(None, read_only=True)
    assert context.operation == "read"
    assert context.read_only is True


def test_using_context_manager_sets_and_resets_markers():
    with using("analytics", shard_key=42):
        context = RoutingContext.create()
        assert context.hint == "analytics"
        assert context.shard_key == 42
    context = RoutingContext.create()
    assert context.hint is None
    assert context.shard_key is None


def test_using_nested_inner_wins():
    with using("outer"), using("inner"):
        assert RoutingContext.create().hint == "inner"


def test_merge_read_only_is_tri_state():
    options = RoutingOptions(read_only=True)
    assert options.merge().read_only is True
    assert options.merge(read_only=False).read_only is False
    assert RoutingOptions().merge(read_only=True).read_only is True


def test_using_nested_read_only_false_forces_primary():
    with using(read_only=True):
        assert RoutingContext.create().read_only is True
        with using(read_only=False):
            assert RoutingContext.create().read_only is False
        assert RoutingContext.create().read_only is True


def test_explicit_arguments_win_over_using():
    with using("ambient", shard_key=1):
        context = RoutingContext.create(hint="explicit", shard_key=2)
    assert context.hint == "explicit"
    assert context.shard_key == 2


def test_using_marker_is_reusable_sequentially():
    marker = using(read_only=True)
    with marker:
        assert RoutingContext.create().read_only is True
    assert RoutingContext.create().read_only is False
    with marker:
        assert RoutingContext.create().read_only is True
    assert RoutingContext.create().read_only is False


def test_using_marker_is_not_reentrant():
    marker = using("analytics")
    with marker, pytest.raises(RuntimeError, match="not reentrant"):
        marker.__enter__()


async def test_using_as_decorator():
    @using("analytics")
    async def decorated():
        return RoutingContext.create().hint

    assert await decorated() == "analytics"
    assert RoutingContext.create().hint is None


async def test_use_context_dependency_applies_and_resets_markers():
    dependency = use_context("other", read_only=True, shard_key=7)
    generator = dependency()
    await generator.__anext__()

    context = RoutingContext.create()
    assert context.hint == "other"
    assert context.read_only is True
    assert context.shard_key == 7

    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()

    context = RoutingContext.create()
    assert context.hint is None
    assert context.read_only is False
    assert context.shard_key is None


async def test_use_context_dependency_routes_statements():
    primary = Database(MEMORY_URL)
    other = Database(MEMORY_URL)
    cluster = DatabaseCluster({"primary": primary, "other": other})

    generator = use_context("other")()
    await generator.__anext__()
    try:
        assert cluster.route() is other
    finally:
        await generator.aclose()
    assert cluster.route() is primary


def test_default_router_unknown_name_raises():
    router = DefaultRouter("missing")
    with pytest.raises(RoutingError, match="missing"):
        router.route({}, RoutingContext.create())


def test_model_router_resolution(user_model):
    class Audited:
        __database__ = "audit"

    router = ModelRouter({user_model: "users", "orders": "orders_db"}, default="main")
    assert router._name_for(user_model) == "users"

    class Order:
        __tablename__ = "orders"

    assert router._name_for(Order) == "orders_db"
    assert router._name_for(Audited) == "audit"
    assert router._name_for(None) == "main"

    class Plain:
        pass

    assert router._name_for(Plain) == "main"


def test_shard_router_mapping_and_callable():
    mapping_router = ShardRouter({0: "shard_0"})
    callable_router = ShardRouter(lambda key: f"shard_{key % 2}")
    databases = {"shard_0": object(), "shard_1": object()}

    context = RoutingContext.create(shard_key=0)
    assert mapping_router.route(databases, context) is databases["shard_0"]
    assert (
        callable_router.route(databases, RoutingContext.create(shard_key=3))
        is (databases["shard_1"])
    )


def test_shard_router_missing_key_raises():
    router = ShardRouter({0: "shard_0"})
    with pytest.raises(RoutingError, match="shard key"):
        router.route({"shard_0": object()}, RoutingContext.create())


def test_shard_router_missing_key_uses_default():
    router = ShardRouter({0: "shard_0"}, default="shard_0")
    databases = {"shard_0": object()}
    assert router.route(databases, RoutingContext.create()) is databases["shard_0"]


def test_shard_router_unknown_shard_raises():
    router = ShardRouter({0: "shard_0"})
    with pytest.raises(RoutingError, match="No shard"):
        router.route({"shard_0": object()}, RoutingContext.create(shard_key=99))


def test_cluster_requires_databases():
    with pytest.raises(ValueError, match="at least one"):
        DatabaseCluster({})


def test_cluster_requires_valid_default():
    with pytest.raises(ValueError, match="Default database"):
        DatabaseCluster({"a": Database(MEMORY_URL)}, default="b")


def test_cluster_rejects_mixed_dialects():
    a = Database(MEMORY_URL)
    b = Database(MEMORY_URL)
    b.dialect = "postgresql"
    with pytest.raises(ValueError, match="dialect"):
        DatabaseCluster({"a": a, "b": b}, default="a")


def test_cluster_route_honors_using_hint():
    a = Database(MEMORY_URL)
    b = Database(MEMORY_URL)
    cluster = DatabaseCluster({"a": a, "b": b}, default="a")
    with cluster.using("b"):
        assert cluster.route() is b
    assert cluster.route() is a


def test_cluster_using_unknown_hint_raises():
    cluster = DatabaseCluster({"a": Database(MEMORY_URL)}, default="a")
    with pytest.raises(RoutingError, match="Unknown database"):
        cluster.using("b")


async def test_cluster_route_unknown_hint_raises():
    cluster = DatabaseCluster({"a": Database(MEMORY_URL)}, default="a")
    with pytest.raises(RoutingError, match="Unknown database"):
        cluster.route(RoutingContext.create(hint="b"))


async def test_transaction_pins_database():
    a = Database(MEMORY_URL)
    b = Database(MEMORY_URL)
    cluster = DatabaseCluster({"a": a, "b": b}, default="a")
    async with cluster.session_context():
        assert cluster.route() is a
        assert cluster.route(RoutingContext.create(hint="a")) is a
    assert cluster.route(RoutingContext.create(hint="b")) is b


async def test_hint_conflicting_with_pinned_transaction_raises():
    cluster = DatabaseCluster(
        {"a": Database(MEMORY_URL), "b": Database(MEMORY_URL)}, default="a"
    )
    async with cluster.session_context():
        with pytest.raises(RoutingError, match="pinned"):
            cluster.route(RoutingContext.create(hint="b"))


async def test_cluster_execute_and_verify_connection():
    cluster = DatabaseCluster({"a": Database(MEMORY_URL)}, default="a")
    await cluster.verify_connection()
    result = await cluster.execute(sa.select(sa.literal(1)))
    assert result.scalar() == 1


async def test_cluster_session_uses_routed_database_without_pinning():
    a = Database(MEMORY_URL)
    b = Database(MEMORY_URL)
    cluster = DatabaseCluster({"a": a, "b": b}, default="a")

    async with cluster.session() as session:
        assert session.get_bind() is a.engine.sync_engine
        assert (await session.execute(sa.select(sa.literal(1)))).scalar() == 1
        # unlike session_context, a fresh session does not pin the database
        assert cluster.route(RoutingContext.create(hint="b")) is b

    async with cluster.session(RoutingContext.create(hint="b")) as session:
        assert session.get_bind() is b.engine.sync_engine


async def test_cluster_atomic():
    cluster = DatabaseCluster({"a": Database(MEMORY_URL)}, default="a")

    @cluster.atomic
    async def do_work():
        return cluster.route()

    assert await do_work() is cluster.default_database


async def test_repository_using_hint_routes_statements(user_model, user_data):
    primary = Database(MEMORY_URL)
    other = Database(MEMORY_URL)
    cluster = DatabaseCluster({"primary": primary, "other": other})

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    await cluster.create_all()
    try:
        repository = Repository().using(db=cluster)
        await repository.create(**user_data)
        await repository.using("other").create(id=uuid4(), name="Jane")

        assert await Repository().using(db=primary).count() == 1
        assert await Repository().using(db=other).count() == 1
        assert (await repository.using("other").get(name="Jane")) is not None
        assert (await repository.get(name="Jane")) is None
    finally:
        await cluster.dispose()


async def test_repository_using_is_preserved_across_chaining(user_model, user_data):
    primary = Database(MEMORY_URL)
    other = Database(MEMORY_URL)
    cluster = DatabaseCluster({"primary": primary, "other": other})

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    await cluster.create_all()
    try:
        await Repository().using("other", db=cluster).create(**user_data)
        users = (
            await Repository()
            .using("other", db=cluster)
            .select()
            .filter(name="John")
            .all()
        )
        assert len(users) == 1
    finally:
        await cluster.dispose()


async def test_model_router_routes_repository_statements(user_model, user_data):
    main = Database(MEMORY_URL)
    audit = Database(MEMORY_URL)
    cluster = DatabaseCluster(
        {"main": main, "audit": audit},
        router=ModelRouter({user_model: "audit"}, default="main"),
        default="main",
    )

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    await cluster.create_all()
    try:
        await Repository().using(db=cluster).create(**user_data)
        assert await Repository().using(db=audit).count() == 1
        assert await Repository().using(db=main).count() == 0
    finally:
        await cluster.dispose()


async def test_uow_shard_key_pins_shard(user_model, user_data):
    shard_0 = Database(MEMORY_URL)
    shard_1 = Database(MEMORY_URL)
    cluster = DatabaseCluster(
        {"shard_0": shard_0, "shard_1": shard_1},
        router=ShardRouter({0: "shard_0", 1: "shard_1"}),
        default="shard_0",
    )

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    class Uow(SQLAlchemyUnitOfWork):
        users: Repository

    await cluster.create_all()
    try:
        async with Uow().using(db=cluster, shard_key=1) as uow:
            user = await uow.users.create(**user_data)
            assert user is not None
        assert await Repository().using(db=shard_1).count() == 1
        assert await Repository().using(db=shard_0).count() == 0
    finally:
        await cluster.dispose()


async def test_atomic_preserves_repository_routing(user_model, user_data):
    primary = Database(MEMORY_URL)
    other = Database(MEMORY_URL)
    cluster = DatabaseCluster({"primary": primary, "other": other})

    class Repository(SQLAlchemyRepository[user_model]):
        @atomic
        async def add_user(self, **values):
            return await self.create(**values)

    await cluster.create_all()
    try:
        await Repository().using("other", db=cluster).add_user(**user_data)
        assert await Repository().using(db=other).count() == 1
        assert await Repository().using(db=primary).count() == 0
    finally:
        await cluster.dispose()


async def test_model_router_routes_uow_statements(user_model, user_data):
    main = Database(MEMORY_URL)
    audit = Database(MEMORY_URL)
    cluster = DatabaseCluster(
        {"main": main, "audit": audit},
        router=ModelRouter({user_model: "audit"}, default="main"),
        default="main",
    )

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    class Uow(SQLAlchemyUnitOfWork):
        users: Repository

    await cluster.create_all()
    try:
        async with Uow().using(db=cluster) as uow:
            user = await uow.users.create(**user_data)
            assert user is not None
        assert await Repository().using(db=audit).count() == 1
        assert await Repository().using(db=main).count() == 0
    finally:
        await cluster.dispose()


async def test_uow_repositories_routing_to_different_databases_raises(user_model):
    main = Database(MEMORY_URL)
    audit = Database(MEMORY_URL)
    cluster = DatabaseCluster(
        {"main": main, "audit": audit},
        router=ModelRouter({user_model: "audit"}, default="main"),
        default="main",
    )

    class Users(SQLAlchemyRepository[user_model]):
        pass

    class Items(SQLAlchemyRepository[RoutedItem]):
        pass

    class Uow(SQLAlchemyUnitOfWork):
        users: Users
        items: Items

    uow = Uow().using(db=cluster)
    with pytest.raises(RoutingError, match="cannot span"):
        async with uow:
            pass  # pragma: no cover


async def test_uow_hint_pins_database(user_model, user_data):
    primary = Database(MEMORY_URL)
    other = Database(MEMORY_URL)
    cluster = DatabaseCluster({"primary": primary, "other": other})

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    class Uow(SQLAlchemyUnitOfWork):
        users: Repository

    await cluster.create_all()
    try:
        async with Uow().using("other", db=cluster) as uow:
            await uow.users.create(**user_data)
        assert await Repository().using(db=other).count() == 1
        assert await Repository().using(db=primary).count() == 0
    finally:
        await cluster.dispose()


def test_explicit_read_only_false_wins_over_ambient_scope():
    with using(read_only=True):
        assert RoutingContext.create().read_only is True
        assert RoutingContext.create(read_only=False).read_only is False


def test_repository_read_only_false_overrides_ambient_scope(user_model):
    cluster = DatabaseCluster.with_replicas(MEMORY_URL, read_replicas=[MEMORY_URL])

    class Repository(SQLAlchemyRepository[user_model]):
        pass

    repo = Repository().using(db=cluster)
    with using(read_only=True):
        assert cluster.route(repo.routing_context()) in cluster.replicas
        forced = repo.using(read_only=False).routing_context()
        assert cluster.route(forced) is cluster.default_database


async def test_read_only_uow_enters_cluster_with_multiple_replicas(user_model):
    cluster = DatabaseCluster.with_replicas(
        MEMORY_URL,
        read_replicas=[MEMORY_URL, MEMORY_URL],
        replica_strategy="round_robin",
    )

    class Users(SQLAlchemyRepository[user_model]):
        pass

    class Items(SQLAlchemyRepository[RoutedItem]):
        pass

    class Uow(SQLAlchemyUnitOfWork):
        users: Users
        items: Items

    # replicas are interchangeable: the cross-database check must not flag
    # the round-robin strategy picking a different replica per repository
    async with Uow().using(db=cluster, read_only=True) as uow:
        assert uow.session is not None


async def test_atomic_supports_objects_with_only_db_property():
    database = Database(MEMORY_URL)

    class Service:
        db = database

        @atomic
        async def op(self) -> str:
            return "done"

    assert await Service().op() == "done"
