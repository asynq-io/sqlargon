from sqlargon.uow import SQLAlchemyUnitOfWork


async def test_uow(db, user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow(db)
    assert isinstance(uow.users, user_repository_class)
    assert uow.users.db is db


async def test_uow_context_manager(db, user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow(db)
    async with uow:
        assert isinstance(uow.users, user_repository_class)
        await uow.users.all()
