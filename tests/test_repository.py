from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import Result
from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from sqlalchemy.orm import aliased, relationship

from sqlargon import Base, Database, SQLAlchemyRepository
from sqlargon.functools import atomic


# Declared at module level: pytest-repeat re-runs every test, and a model
# declared inside a test would clash with itself on the second run.
class CamelCaseUser(Base):
    id = sa.Column(sa.Integer, primary_key=True)


class Team(Base):
    __tablename__ = "test_load_team"
    id = sa.Column(sa.Integer, primary_key=True)
    members = relationship("Member", back_populates="team")


class Member(Base):
    __tablename__ = "test_load_member"
    id = sa.Column(sa.Integer, primary_key=True)
    team_id = sa.Column(sa.ForeignKey("test_load_team.id"))
    team = relationship(Team, back_populates="members")


class TeamRepository(SQLAlchemyRepository[Team]):
    pass


@pytest.fixture
async def team_tables(db: Database):
    async with db.engine.begin() as conn:
        await conn.run_sync(Team.__table__.create, checkfirst=True)
        await conn.run_sync(Member.__table__.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        await conn.run_sync(Member.__table__.drop, checkfirst=True)
        await conn.run_sync(Team.__table__.drop, checkfirst=True)


async def test_empty_repo(user_repository):
    users = await user_repository.all()
    assert users == []


async def test_get_nonexistent(user_repository):
    user = await user_repository.get(id=uuid4())
    assert user is None


async def test_create(user_repository):
    user = await user_repository.create(name="John")
    assert user is not None
    assert user.name == "John"


async def test_create_duplicate(user_repository, user_data):
    user1 = await user_repository.create(**user_data)
    assert user1 is not None
    user2 = await user_repository.create(**user_data)
    assert user2 is None


async def test_create_or_update(user_repository):
    user = await user_repository.create_or_update(name="John")
    assert user.name == "John"


async def test_create_or_update_idempotent(user_repository, user_data):
    user1 = await user_repository.create_or_update(**user_data)
    assert user1.id == user_data["id"]
    user2 = await user_repository.create_or_update(**user_data)
    assert user2.id == user1.id


async def test_get_or_create(user_repository, user_data):
    user1 = await user_repository.get_or_create(**user_data)
    assert user1.id == user_data["id"]
    user2 = await user_repository.get_or_create(**user_data)
    assert user2.id == user1.id


async def test_list_all(user_repository):
    await user_repository.create(name="Alice")
    await user_repository.create(name="Bob")
    users = await user_repository.list()
    assert len(users) == 2


async def test_list_with_filter(user_repository):
    await user_repository.create(name="Alice")
    await user_repository.create(name="Bob")
    model = user_repository.model
    users = await user_repository.list(model.name == "Alice")
    assert len(users) == 1
    assert users[0].name == "Alice"


async def test_insert(user_repository):
    await user_repository.insert([{"name": "John"}, {"name": "Jane"}]).execute()
    assert await user_repository.count() == 2


async def test_upsert(user_repository):
    users = [
        {"name": "John", "last_name": "Connor"},
        {"name": "Vincent", "last_name": "Carter"},
    ]
    await user_repository.upsert(users).execute()
    users_updated = [
        {"name": "John", "last_name": None},
        {"name": "Vincent", "last_name": None},
    ]
    await user_repository.upsert(users_updated).execute()
    model = user_repository.model
    assert await user_repository.count(model.last_name.in_(("Connor", "Carter"))) == 2


async def test_bulk_create(user_repository):
    users = [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]
    await user_repository.bulk_create(users)
    assert await user_repository.count() == 3


async def test_bulk_create_with_return(user_repository):
    users = [{"name": "Alice"}, {"name": "Bob"}]
    result = await user_repository.bulk_create(users, return_results=True)
    assert result is not None
    assert len(result) == 2


async def test_bulk_create_or_update(user_repository):
    users = [{"name": "Alice"}, {"name": "Bob"}]
    await user_repository.bulk_create_or_update(users)
    assert await user_repository.count() == 2


async def test_bulk_update(user_repository):
    await user_repository.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()
    await user_repository.bulk_update(
        values=[
            {"name": "John", "last_name": "Connor"},
            {"name": "Vincent", "last_name": "Carter"},
        ],
        on_={"name"},
    )
    model = user_repository.model
    assert await user_repository.count(model.last_name.in_(("Connor", "Carter"))) == 2


async def test_bulk_update_returns_none(user_repository):
    await user_repository.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()
    # an executemany cannot return rows, so bulk_update never does
    result = await user_repository.bulk_update(
        values=[
            {"name": "John", "last_name": "Connor"},
            {"name": "Vincent", "last_name": "Carter"},
        ],
        on_={"name"},
    )
    assert result is None
    model = user_repository.model
    assert await user_repository.count(model.last_name.in_(("Connor", "Carter"))) == 2


async def test_update_one(user_repository, user_data):
    await user_repository.create(**user_data)
    model = user_repository.model
    user = await user_repository.update_one(
        {"last_name": "Connor"}, model.id == user_data["id"]
    )
    assert user is not None
    assert user.last_name == "Connor"


async def test_delete_one(user_repository, user_data):
    await user_repository.create(**user_data)
    model = user_repository.model
    user = await user_repository.delete_one(model.id == user_data["id"])
    assert user is not None
    assert user.id == user_data["id"]
    assert await user_repository.count() == 0


async def test_delete_many(user_repository):
    await user_repository.insert([{"name": "Alice"}, {"name": "Bob"}]).execute()
    deleted = await user_repository.delete_many()
    assert len(deleted) == 2
    assert await user_repository.count() == 0


async def test_remove(user_repository, user_data):
    await user_repository.create(**user_data)
    model = user_repository.model
    await user_repository.remove(model.id == user_data["id"])
    assert await user_repository.count() == 0


async def test_count(user_repository):
    assert await user_repository.count() == 0
    await user_repository.insert([{"name": "Alice"}, {"name": "Bob"}]).execute()
    assert await user_repository.count() == 2


async def test_count_with_filter(user_repository):
    await user_repository.insert([{"name": "Alice"}, {"name": "Bob"}]).execute()
    model = user_repository.model
    assert await user_repository.count(model.name == "Alice") == 1


async def test_count_with_kwargs(user_repository):
    await user_repository.insert([{"name": "Alice"}, {"name": "Bob"}]).execute()
    assert await user_repository.count(name="Alice") == 1


async def test_filter_by_kwargs(user_repository):
    await user_repository.create(name="Alice")
    await user_repository.create(name="Bob")
    users = await user_repository.filter(name="Alice").all()
    assert len(users) == 1
    assert users[0].name == "Alice"


async def test_select_columns(user_repository):
    await user_repository.create(name="Alice")
    model = user_repository.model
    rows = await user_repository.select(model.name).mappings()
    result = rows.all()
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


async def test_one(user_repository):
    await user_repository.create(name="Alice")
    user = await user_repository.select().one()
    assert user.name == "Alice"


async def test_one_raises_on_empty(user_repository):
    with pytest.raises(NoResultFound):
        await user_repository.select().one()


async def test_first(user_repository):
    await user_repository.create(name="Alice")
    user = await user_repository.select().first()
    assert user is not None


async def test_first_empty(user_repository):
    result = await user_repository.select().first()
    assert result is None


async def test_mappings(user_repository):
    await user_repository.create(name="Alice")
    result = await user_repository.select().mappings()
    rows = result.all()
    assert len(rows) == 1


async def test_scalar(user_repository):
    await user_repository.create(name="Alice")
    model = user_repository.model
    result = await user_repository.select(model.name).scalar()
    assert result == "Alice"


async def test_stream(user_repository):
    await user_repository.insert([{"name": "Alice"}, {"name": "Bob"}]).execute()
    items = [row async for row in user_repository.select().stream()]
    assert len(items) == 2


async def test_all_unique(user_repository):
    await user_repository.create(name="Alice")
    users = await user_repository.select().all(unique=True)
    assert len(users) == 1


async def test_default_order_by(user_repository):
    await user_repository.insert(
        [{"name": "Alice"}, {"name": "Charlie"}, {"name": "Bob"}]
    ).execute()
    users = await user_repository.all()
    assert [u.name for u in users] == ["Charlie", "Bob", "Alice"]


async def test_await_repository(user_repository):
    await user_repository.create(name="Alice")
    result = await user_repository.select()
    assert isinstance(result, Result)


async def test_proxy_chain(user_repository):
    await user_repository.insert([{"name": "Alice"}, {"name": "Bob"}]).execute()
    users = await user_repository.select().limit(1).all()
    assert len(users) == 1


async def test_init_subclass_invalid_model():
    with pytest.raises(TypeError, match="Could not resolve model"):

        class BadRepo(SQLAlchemyRepository[str]):  # type: ignore[type-var]
            pass


async def test_repository_database_binding(user_repository_class):
    other_db = Database("sqlite+aiosqlite:///:memory:")

    repo = user_repository_class().using(db=other_db)
    assert repo.db is other_db
    assert user_repository_class().db is not other_db


async def test_repository_database_binding_preserved_across_chaining(
    user_repository_class,
):
    other_db = Database("sqlite+aiosqlite:///:memory:")

    repo = user_repository_class().using(db=other_db)
    assert repo.filter(name="John").db is other_db


async def test_atomic_decorator(user_repository_class):
    class Repo(user_repository_class):
        @atomic
        async def create_users(self, names):
            for name in names:
                await self.create(name=name)

    repo = Repo()
    await repo.create_users(["Alice", "Bob"])
    assert await repo.count() == 2


def test_tablename_is_derived_from_camel_case_class_name():
    assert CamelCaseUser.__tablename__ == "camel_case_user"


@pytest.mark.parametrize(
    ("isouter", "expected"), [(False, ["Alice"]), (True, ["Alice", "Bob"])]
)
async def test_join(user_repository, user_model, isouter, expected):
    await user_repository.insert(
        [{"name": "Alice", "last_name": "Bob"}, {"name": "Bob", "last_name": None}]
    ).execute()
    manager = aliased(user_model)
    users = await (
        user_repository.select()
        .join(manager, manager.name == user_model.last_name, isouter=isouter)
        .all()
    )
    assert sorted(user.name for user in users) == expected


async def test_execute_many(user_repository, user_model):
    qb = user_repository.qb
    await user_repository.execute_many(
        qb.insert(user_model, {"name": "Alice"}),
        qb.insert(user_model, {"name": "Bob"}),
    )
    assert await user_repository.count() == 2


async def test_update_many(user_repository):
    await user_repository.insert([{"name": "Alice"}, {"name": "Bob"}]).execute()
    await user_repository.update_many({"last_name": "Smith"}, name="Alice")
    alice = await user_repository.get(name="Alice")
    bob = await user_repository.get(name="Bob")
    assert alice is not None
    assert alice.last_name == "Smith"
    assert bob is not None
    assert bob.last_name is None


async def test_bulk_create_or_update_with_return(user_repository):
    users = [{"name": "Alice"}, {"name": "Bob"}]
    result = await user_repository.bulk_create_or_update(users, return_results=True)
    assert {user.name for user in result} == {"Alice", "Bob"}


async def test_get_chunk_for_update_applies_values(user_repository, user_model):
    await user_repository.insert(
        [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]
    ).execute()

    async with user_repository.get_chunk_for_update(
        {"last_name": "processed"}, 2, "id", user_model.name
    ) as chunk:
        assert [user.name for user in chunk] == ["Alice", "Bob"]
        assert all(user.last_name is None for user in chunk)

    processed = await user_repository.filter(last_name="processed").all()
    assert {user.name for user in processed} == {"Alice", "Bob"}
    assert await user_repository.count(last_name=None) == 1


async def test_get_chunk_for_update_removes_chunk_without_values(
    user_repository, user_model
):
    await user_repository.insert(
        [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}]
    ).execute()

    async with user_repository.get_chunk_for_update(
        None, 2, "id", user_model.name
    ) as chunk:
        assert [user.name for user in chunk] == ["Alice", "Bob"]
        assert await user_repository.count() == 3

    assert [user.name for user in await user_repository.all()] == ["Charlie"]


async def test_get_chunk_for_update_filters_and_orders_by_primary_key(user_repository):
    await user_repository.insert(
        [{"name": "Alice", "last_name": "new"}, {"name": "Bob", "last_name": "done"}]
    ).execute()

    async with user_repository.get_chunk_for_update(
        {"last_name": "done"}, 10, "id", None, last_name="new"
    ) as chunk:
        assert [user.name for user in chunk] == ["Alice"]

    assert await user_repository.count(last_name="done") == 2


async def test_get_chunk_for_update_on_empty_table(user_repository):
    async with user_repository.get_chunk_for_update({"last_name": "done"}) as chunk:
        assert list(chunk) == []

    assert await user_repository.count() == 0


@pytest.mark.usefixtures("team_tables")
async def test_load_eagerly_loads_relationship(db: Database):
    repository = TeamRepository()
    await repository.insert({"id": 1}).execute()
    await db.execute(
        sa.insert(Member).values([{"id": 1, "team_id": 1}, {"id": 2, "team_id": 1}])
    )

    teams = await repository.load(Team.members).all()

    # the session is closed by now, so members are only reachable if eagerly loaded
    assert [len(team.members) for team in teams] == [2]


@pytest.mark.usefixtures("team_tables")
async def test_relationship_is_lazy_without_load():
    repository = TeamRepository()
    await repository.insert({"id": 1}).execute()

    team = await repository.select().one()

    with pytest.raises(SQLAlchemyError):
        _ = team.members
