import pytest
import sqlalchemy as sa

from sqlargon import Database, SQLAlchemyUnitOfWork
from sqlargon.functools import atomic

from .models import User, UserRepository


class Failure(RuntimeError):
    pass


class UsersUow(SQLAlchemyUnitOfWork):
    users: UserRepository


class FailingUserRepository(UserRepository):
    @atomic
    async def add_two_then_fail(self) -> None:
        await self.insert({"name": "John"}).execute()
        await self.insert({"name": "Vincent"}).execute()
        raise Failure


async def insert_then_fail(db: Database, users: UserRepository, *names: str) -> None:
    async with db.session_context():
        for name in names:
            await users.insert({"name": name}).execute()
        raise Failure


async def insert_nested_then_fail(db: Database, users: UserRepository) -> None:
    async with db.session_context():
        await users.insert({"name": "John"}).execute()
        async with db.session_context():
            await users.insert({"name": "Vincent"}).execute()
        raise Failure


async def insert_in_uow_then_fail(db: Database) -> None:
    async with UsersUow().using(db=db) as uow:
        await uow.users.insert({"name": "John"}).execute()
        raise Failure


async def test_session_commits_on_success(db: Database, users: UserRepository):
    async with db.session_context():
        await users.insert({"name": "John"}).execute()
    assert await users.count() == 1


async def test_session_rolls_back_on_error(db: Database, users: UserRepository):
    with pytest.raises(Failure):
        await insert_then_fail(db, users, "John")
    assert await users.count() == 0


async def test_nested_session_context_shares_one_transaction(
    db: Database, users: UserRepository
):
    with pytest.raises(Failure):
        await insert_nested_then_fail(db, users)
    assert await users.count() == 0


async def test_atomic_rolls_the_whole_method_back(users: UserRepository):
    with pytest.raises(Failure):
        await FailingUserRepository().add_two_then_fail()
    assert await users.count() == 0


async def test_uncommitted_rows_are_invisible_to_another_connection(
    db: Database, database_url: str, users: UserRepository
):
    other = Database(database_url)
    try:
        async with db.session_context():
            await users.insert({"name": "John"}).execute()
            visible = (
                await other.execute(sa.select(sa.func.count()).select_from(User))
            ).scalar()
        assert visible == 0
    finally:
        await other.dispose()
    assert await users.count() == 1


async def test_unit_of_work_commits(db: Database, users: UserRepository):
    async with UsersUow().using(db=db) as uow:
        await uow.users.insert({"name": "John"}).execute()
    assert await users.count() == 1


async def test_unit_of_work_rolls_back_on_error(db: Database, users: UserRepository):
    with pytest.raises(Failure):
        await insert_in_uow_then_fail(db)
    assert await users.count() == 0


async def test_unit_of_work_explicit_rollback(db: Database, users: UserRepository):
    async with UsersUow().using(db=db) as uow:
        await uow.users.insert({"name": "John"}).execute()
        await uow.rollback()
    assert await users.count() == 0
