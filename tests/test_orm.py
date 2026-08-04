from sqlargon import Base, Database


def test_eager_defaults_enabled():
    assert Base.__mapper_args__["eager_defaults"] is True


async def test_server_default_loaded_after_commit(db: Database, user_model):
    await db.create_all()
    try:
        async with db.session() as session:
            user = user_model(name="John")
            session.add(user)

        # without eager_defaults, accessing the server-generated primary key
        # after the session is closed raises MissingGreenlet
        assert user.id is not None
    finally:
        await db.drop_all()
