import pytest

from sqlargon import Database
from sqlargon.uow import SQLAlchemyUnitOfWork


@pytest.mark.usefixtures("db")
async def test_uow_basic(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    async with uow:
        assert isinstance(uow.users, user_repository_class)


async def test_uow_db_property(db: Database, user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    assert uow.db is db


@pytest.mark.usefixtures("db")
async def test_uow_session_available(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    async with uow:
        assert uow.session is not None


async def test_uow_list_users(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    async with uow:
        assert isinstance(uow.users, user_repository_class)
        await uow.users.all()


async def test_uow_create_user(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    async with uow:
        user = await uow.users.create(name="John")
        assert user is not None
        assert user.name == "John"


async def test_uow_nested_subclass(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    class NestedTestUow(TestUow):
        nested_users: user_repository_class

    uow = NestedTestUow()
    async with uow:
        assert isinstance(uow.users, user_repository_class)
        assert isinstance(uow.nested_users, user_repository_class)


async def test_uow_session_not_initialized(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    with pytest.raises(ValueError, match="Session not initialized"):
        _ = uow.session


async def test_uow_double_enter_raises(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    async with uow:
        with pytest.raises(ValueError, match="Cannot open"):
            await uow.__aenter__()


async def test_uow_database_not_initialized():
    class TestUow(SQLAlchemyUnitOfWork):
        pass

    uow = TestUow()
    uow.use_db("nonexistent_db")
    with pytest.raises(KeyError):
        _ = uow.db


async def test_uow_commit(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    async with uow:
        await uow.commit()


async def test_uow_rollback(user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    async with uow:
        await uow.rollback()


async def test_uow_init_subclass_with_database(db: Database, user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow()
    assert uow.db is db


async def test_uow_unresolved_annotation():
    class TestUow(SQLAlchemyUnitOfWork):
        pass

    uow = TestUow()
    async with uow:
        with pytest.raises(AttributeError):
            _ = uow.nonexistent_repo  # type: ignore[attr-defined]
