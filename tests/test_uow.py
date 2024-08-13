from .conftest import UserRepository


async def test_uow(user_uow) -> None:
    assert isinstance(user_uow.users, UserRepository)
    assert user_uow.users.db is user_uow.db


async def test_uow_context_manager(user_uow) -> None:
    async with user_uow:
        all_users = await user_uow.users.all()
        assert all_users == []
