from uuid import uuid4


def test_supports_returning(user_repository):
    assert user_repository.supports_returning


async def test_empty_user_repo(user_repository):
    users = await user_repository.all()
    assert users == []


async def test_get_user(user_repository):
    user = await user_repository.get(id=uuid4())
    assert user is None


async def test_create_user(user_repository):
    name = "John"
    user = await user_repository.create(name=name)
    assert user.name == name


async def test_create_safe(user_repository, user_data):
    user1 = await user_repository.create(**user_data)
    assert user1.id == user_data["id"]
    user2 = await user_repository.create(**user_data)
    assert user2 is None


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
    assert 2 == await user_repository.count(
        user_repository.model.last_name.in_(("Connor", "Carter"))
    )
