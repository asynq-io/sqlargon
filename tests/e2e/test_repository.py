from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from .backends import Backend
from .models import (
    ExcludingUserRepository,
    User,
    UserByNameRepository,
    UserRepository,
)

NAMES = ("Andrew", "John", "Vincent")


async def seed(users: UserRepository, *names: str, score: int = 0) -> None:
    """Insert rows without relying on a RETURNING clause."""
    await users.bulk_create(
        [{"name": name, "score": score} for name in names], return_results=False
    )


async def test_insert_and_read_back(users: UserRepository):
    await seed(users, "John")
    user = await users.get(name="John")
    assert user is not None
    assert isinstance(user.id, UUID)
    assert user.score == 0


async def test_client_side_defaults_are_applied(users: UserRepository):
    await seed(users, "John")
    user = await users.get(name="John")
    assert user is not None
    assert user.created_at.tzinfo is not None
    assert user.is_new


async def test_all_is_ordered_by_the_default_order_by(users: UserRepository):
    await seed(users, *reversed(NAMES))
    assert [user.name for user in await users.all()] == list(NAMES)


async def test_count(users: UserRepository):
    await seed(users, *NAMES)
    assert await users.count() == len(NAMES)
    assert await users.count(User.name == "John") == 1
    assert await users.count(name="nobody") == 0


async def test_get_missing_row(users: UserRepository):
    assert await users.get(name="nobody") is None


async def test_unique_constraint_is_enforced(users: UserRepository):
    await seed(users, "John")
    with pytest.raises(IntegrityError):
        await users.insert({"name": "John"}).execute()


async def test_bulk_create_ignores_conflicts(users: UserRepository):
    await seed(users, "John")
    user = await users.get(name="John")
    assert user is not None
    await users.bulk_create(
        [{"id": user.id, "name": "John", "score": 99}], return_results=False
    )
    assert await users.count() == 1
    refreshed = await users.get(name="John")
    assert refreshed is not None
    assert refreshed.score == 0


async def test_upsert_updates_the_conflicting_row(users: UserRepository):
    await seed(users, "John")
    user = await users.get(name="John")
    assert user is not None
    # every column of the default conflict set is named, so the statement is
    # portable -- see test_capabilities.test_upsert_of_a_partial_row
    await users.upsert(
        {"id": user.id, "name": "John", "last_name": "Doe", "score": 7}
    ).execute()
    refreshed = await users.get(name="John")
    assert refreshed is not None
    assert refreshed.score == 7


async def test_update_without_returning(users: UserRepository):
    await seed(users, *NAMES)
    await users.update({"score": 5}).where(User.name == "John").execute()
    john = await users.get(name="John")
    assert john is not None
    assert john.score == 5
    assert await users.count(User.score == 0) == len(NAMES) - 1


async def test_bulk_update(users: UserRepository):
    await seed(users, *NAMES)
    rows = await users.list()
    await users.bulk_update([{"id": row.id, "score": 3} for row in rows])
    assert await users.count(User.score == 3) == len(NAMES)


async def test_remove(users: UserRepository):
    await seed(users, *NAMES)
    await users.remove(User.name == "John")
    assert await users.count() == len(NAMES) - 1
    assert await users.get(name="John") is None


async def test_delete_many(users: UserRepository):
    await seed(users, *NAMES)
    await users.delete_many(User.name.in_(["John", "Andrew"]))
    assert [user.name for user in await users.all()] == ["Vincent"]


async def test_stream_yields_every_row(users: UserRepository):
    await seed(users, *NAMES)
    streamed = [row[0].name async for row in users.select().stream()]
    assert sorted(streamed) == sorted(NAMES)


async def test_scalars_and_first(users: UserRepository):
    await seed(users, *NAMES)
    first = await users.first()
    assert first is not None
    assert first.name == NAMES[0]
    assert len(await users.all()) == len(NAMES)


async def test_mappings(users: UserRepository):
    await seed(users, "John")
    rows = (await users.select(User.name).mappings()).all()
    assert [row["name"] for row in rows] == ["John"]


async def insert_twice(users: UserRepository, name: str) -> None:
    async with users.session():
        await users.insert({"name": name}).execute()
        await users.insert({"name": name}).execute()


async def test_transaction_rolls_back_on_error(users: UserRepository):
    with pytest.raises(IntegrityError):
        await insert_twice(users, "John")
    assert await users.count() == 0


async def test_create_returns_the_inserted_row(users: UserRepository):
    user = await users.create(name="John", last_name="Doe")
    assert user is not None
    assert isinstance(user.id, UUID)
    assert user.name == "John"
    assert user.created_at == user.updated_at


async def test_create_returns_none_on_conflict(users: UserRepository):
    user_id = uuid4()
    assert await users.create(id=user_id, name="John") is not None
    assert await users.create(id=user_id, name="John") is None


@pytest.mark.usefixtures("needs_partial_upsert")
async def test_create_or_update_is_idempotent(users: UserRepository):
    user_id = uuid4()
    first = await users.create_or_update(id=user_id, name="John")
    second = await users.create_or_update(id=user_id, name="Johnny")
    assert first.id == second.id == user_id
    assert second.name == "Johnny"
    assert await users.count() == 1


async def test_on_conflict_exclude_set_keeps_the_stored_column():
    repository = ExcludingUserRepository()
    user = await repository.create_or_update(id=uuid4(), name="John", score=1)
    updated = await repository.create_or_update(id=user.id, name="Johnny", score=42)
    assert updated.name == "Johnny"
    assert updated.score == 1


async def test_get_or_create():
    repository = UserByNameRepository()
    created = await repository.get_or_create({"score": 5}, name="John")
    existing = await repository.get_or_create({"score": 9}, name="John")
    assert created.id == existing.id
    assert existing.score == 5


async def test_create_many_returns_every_row(users: UserRepository):
    created = await users.create_many([{"name": name} for name in NAMES])
    assert sorted(user.name for user in created) == sorted(NAMES)


@pytest.mark.usefixtures("needs_partial_upsert")
async def test_bulk_create_or_update_returns_rows(users: UserRepository):
    await seed(users, *NAMES)
    rows = await users.list()
    updated = await users.bulk_create_or_update(
        [{"id": row.id, "name": row.name, "score": 8} for row in rows],
        return_results=True,
    )
    assert {row.score for row in updated} == {8}


async def test_update_one_returns_the_updated_row(users: UserRepository):
    await seed(users, "John")
    updated = await users.update_one({"score": 4}, User.name == "John")
    assert updated is not None
    assert updated.score == 4


async def test_update_many_returns_every_updated_row(users: UserRepository):
    await seed(users, *NAMES)
    updated = await users.update_many({"score": 2}, User.name.in_(NAMES[:2]))
    assert len(updated) == 2


async def test_delete_one_returns_the_deleted_row(users: UserRepository):
    await seed(users, "John")
    deleted = await users.delete_one(User.name == "John")
    assert deleted is not None
    assert deleted.name == "John"
    assert await users.count() == 0


async def test_get_chunk_for_update_removes_the_chunk(users: UserRepository):
    await seed(users, *NAMES)
    async with users.get_chunk_for_update(None, limit=2) as chunk:
        assert len(chunk) == 2
    assert await users.count() == 1


async def test_get_chunk_for_update_applies_values(users: UserRepository):
    await seed(users, *NAMES)
    async with users.get_chunk_for_update({"score": 6}, limit=2) as chunk:
        assert len(chunk) == 2
    assert await users.count(User.score == 6) == 2


async def test_written_rows_come_back_without_a_returning_clause(
    users: UserRepository, backend: Backend
):
    """One create/update/delete round trip over the re-read fallback.

    The assertions above cover the same methods on every backend; this one
    names the backends that reach them without a RETURNING clause, so the
    fallback is exercised deliberately rather than only in passing.
    """
    if backend.dialect != "mysql":
        pytest.skip(f"{backend.name} writes and returns in one statement")
    created = await users.create(name="John")
    assert created is not None
    updated = await users.update_one({"score": 4}, User.name == "John")
    assert updated is not None
    assert updated.id == created.id
    assert updated.score == 4
    deleted = await users.delete_one(User.name == "John")
    assert deleted is not None
    assert deleted.id == created.id
    assert await users.count() == 0
