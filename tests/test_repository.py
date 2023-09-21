from uuid import uuid4


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


async def test_get_or_create(user_repository, user_data):
    user1 = await user_repository.get_or_create(**user_data)
    user2 = await user_repository.get_or_create(**user_data)
    assert user1 is user2


async def test_create_safe(user_repository, user_data):
    user1 = await user_repository.create(**user_data)
    assert user1.id == user_data["id"]
    user2 = await user_repository.create(**user_data)
    assert user2 is None
