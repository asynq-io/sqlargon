from uuid import uuid4

import pytest
from sqlalchemy import Result
from sqlalchemy.exc import NoResultFound

from sqlargon import Database, SQLAlchemyRepository
from sqlargon.functools import atomic
from sqlargon.registry import db_registry


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


async def test_ensure_db_no_overwrite(user_repository_class):
    other_db = Database(
        "sqlite+aiosqlite:///:memory:", name="other_test_db", enable_tracker=False
    )
    try:
        repo = user_repository_class()
        repo.use_db("other_test_db")
        assert repo.db is other_db
    finally:
        db_registry.unregister("other_test_db")


async def test_atomic_decorator(user_repository_class):
    class Repo(user_repository_class):
        @atomic
        async def create_users(self, names):
            for name in names:
                await self.create(name=name)

    repo = Repo()
    await repo.create_users(["Alice", "Bob"])
    assert await repo.count() == 2
