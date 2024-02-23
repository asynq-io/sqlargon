from sqlargon.uow import SQLAlchemyUnitOfWork


async def test_uow(db, user_repository_class):
    class TestUow(SQLAlchemyUnitOfWork):
        users: user_repository_class

    uow = TestUow(db)
    assert isinstance(uow.users, user_repository_class)
    assert uow.users.db is db
