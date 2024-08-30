from typing import Any

import pytest

from sqlargon import Database
from sqlargon.uow import SQLAlchemyUnitOfWork

pytestmark = pytest.mark.anyio


async def test_uow(db: Database, user_repository_class: Any) -> None:
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow(db)
    async with uow:
        assert isinstance(uow.users, user_repository_class)
        assert uow.users.db is db


async def test_uow_context_manager(db: Database, user_repository_class: Any) -> None:
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow(db)
    async with uow:
        assert isinstance(uow.users, user_repository_class)
        await uow.users.all()


async def test_uow_create_user(db: Database, user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow(db)
    async with uow:
        user = await uow.users.create(name="John")
        assert user.name == "John"
