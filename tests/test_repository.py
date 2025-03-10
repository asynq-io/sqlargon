from uuid import uuid4

import pytest

pytestmark = pytest.mark.anyio


async def test_empty_user_repo(user_repository):
    users = await user_repository.all()
    assert users == []


async def test_get_user(user_repository):
    user = await user_repository.get(id=uuid4())
    assert user is None


async def test_create_user(user_repository):
    name = "John"
    user = await user_repository.create_or_update(name=name)
    assert user.name == name


async def test_create_safe(user_repository, user_data):
    user1 = await user_repository.create_or_update(**user_data)
    assert user1.id == user_data["id"]
    user2 = await user_repository.create_or_update(**user_data)
    assert user2.id == user1.id


async def test_bulk_update(user_repository):
    users = [
        {"name": "John"},
        {"name": "Vincent"},
        {"name": "Andrew"},
    ]
    users_update = [
        {"name": "John", "last_name": "Connor"},
        {"name": "Vincent", "last_name": "Carter"},
    ]
    await user_repository.insert(users)
    await user_repository.bulk_update(values=users_update, on_={"name"})
    assert (
        await user_repository.count(
            user_repository.model.last_name.in_(("Connor", "Carter"))
        )
        == 2
    )


async def test_upsert(user_repository):
    users = [
        {"name": "John", "last_name": "Connor"},
        {"name": "Vincent", "last_name": "Carter"},
    ]
    await user_repository.upsert(users)

    users = [
        {"name": "John", "last_name": None},
        {"name": "Vincent", "last_name": None},
    ]
    await user_repository.upsert(users)
    assert (
        await user_repository.count(
            user_repository.model.last_name.in_(("Connor", "Carter"))
        )
        == 2
    )
