from uuid import uuid4


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


async def test_paginate_empty_repository(user_repository):
    page = await user_repository.get_page()
    assert len(page) == 0
    assert page.paging.has_next is False


async def test_paginate_repository(user_repository):
    users = [
        {"name": "John"},
        {"name": "Vincent"},
        {"name": "Andrew"},
    ]
    await user_repository.insert(users)
    page = await user_repository.get_page(page_size=2)
    assert len(page) == 2
    assert page.paging.has_next is True
    assert page.paging.bookmark_next == ">s:John"
