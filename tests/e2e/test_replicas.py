import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from sqlargon import (
    DatabaseCluster,
    ReadOnlyDatabase,
    ReadOnlyError,
    SQLAlchemyRepository,
)

from .models import User, UserRepository


@pytest.fixture
def cluster(database_url: str) -> DatabaseCluster:
    return DatabaseCluster.with_replicas(
        database_url, read_replicas=[database_url], auto_route=True
    )


async def test_replica_serves_reads(cluster: DatabaseCluster, users: UserRepository):
    await users.bulk_create([{"name": "John"}], return_results=False)
    repository: SQLAlchemyRepository = UserRepository().using(db=cluster)
    try:
        assert [user.name for user in await repository.all()] == ["John"]
    finally:
        await cluster.dispose()


async def test_writes_go_to_the_primary(cluster: DatabaseCluster):
    repository: SQLAlchemyRepository = UserRepository().using(db=cluster)
    try:
        await repository.insert({"name": "John"}).execute()
        assert await repository.count() == 1
    finally:
        await cluster.dispose()


async def test_replica_rejects_a_compiled_write(database_url: str):
    replica = ReadOnlyDatabase(database_url)
    try:
        with pytest.raises(ReadOnlyError):
            await replica.execute(sa.insert(User).values(name="John"))
    finally:
        await replica.dispose()


async def test_replica_serves_a_select(database_url: str):
    replica = ReadOnlyDatabase(database_url)
    try:
        result = await replica.execute(sa.select(sa.literal(1)))
        assert result.scalar() == 1
    finally:
        await replica.dispose()


# spelled out rather than interpolated from the model, so the linters can see
# it is not built from input; the assertion below catches a table rename
INSERT_USER = (
    "INSERT INTO e2e_user (id, name) "
    "VALUES ('0f2a1f96-0000-4000-8000-000000000000', 'John')"
)


async def write_past_the_client_guard(replica: ReadOnlyDatabase) -> None:
    """Issue a write as driver level SQL, which the client guard never sees."""
    assert User.__tablename__ in INSERT_USER
    async with replica.session() as session:
        connection = await session.connection()
        await connection.exec_driver_sql(INSERT_USER)


async def test_the_server_rejects_a_write_past_the_client_guard(database_url: str):
    replica = ReadOnlyDatabase(database_url)
    try:
        with pytest.raises(DBAPIError):
            await write_past_the_client_guard(replica)
    finally:
        await replica.dispose()
