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
