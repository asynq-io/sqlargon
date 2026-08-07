import pytest
import sqlalchemy as sa

from sqlargon import Database, DeletedRowExistsError

from .models import SoftUser, SoftUserByNameRepository, SoftUserRepository

NAMES = ("Andrew", "John", "Vincent")


async def seed(soft_users: SoftUserRepository, *names: str) -> None:
    await soft_users.bulk_create(
        [{"name": name} for name in names], return_results=False
    )


async def stored_rows(db: Database) -> dict[str, bool]:
    result = await db.execute(sa.select(SoftUser.name, SoftUser.tombstone))
    return {name: bool(tombstone) for name, tombstone in result.all()}


async def test_remove_tombstones_the_row(db: Database, soft_users: SoftUserRepository):
    await seed(soft_users, *NAMES)
    await soft_users.remove(SoftUser.name == "John")
    assert await stored_rows(db) == {"Andrew": False, "John": True, "Vincent": False}


async def test_reads_skip_tombstoned_rows(soft_users: SoftUserRepository):
    await seed(soft_users, *NAMES)
    await soft_users.remove(SoftUser.name == "John")
    assert [row.name for row in await soft_users.all()] == ["Andrew", "Vincent"]
    assert await soft_users.get(name="John") is None
    assert await soft_users.count() == 2


async def test_with_deleted_covers_tombstoned_rows(soft_users: SoftUserRepository):
    await seed(soft_users, *NAMES)
    await soft_users.remove(SoftUser.name == "John")
    assert await soft_users.with_deleted().count() == len(NAMES)


async def test_only_deleted_is_scoped_to_the_trash(soft_users: SoftUserRepository):
    await seed(soft_users, *NAMES)
    await soft_users.remove(SoftUser.name == "John")
    trashed = await soft_users.only_deleted().all()
    assert [row.name for row in trashed] == ["John"]
    assert await soft_users.only_deleted().count() == 1


async def test_hard_delete_removes_the_row(
    db: Database, soft_users: SoftUserRepository
):
    await seed(soft_users, *NAMES)
    await soft_users.remove(SoftUser.name == "John")
    await soft_users.only_deleted().hard_delete()
    assert set(await stored_rows(db)) == {"Andrew", "Vincent"}


async def test_update_leaves_tombstoned_rows_alone(soft_users: SoftUserRepository):
    await seed(soft_users, "John")
    await soft_users.remove(SoftUser.name == "John")
    await soft_users.update({"name": "Johnny"}).execute()
    assert [row.name for row in await soft_users.only_deleted().all()] == ["John"]


@pytest.mark.usefixtures("needs_update_returning")
async def test_restore_clears_the_tombstone(soft_users: SoftUserRepository):
    await seed(soft_users, *NAMES)
    await soft_users.remove(SoftUser.name == "John")
    restored = await soft_users.restore(SoftUser.name == "John")
    assert [row.name for row in restored] == ["John"]
    assert await soft_users.count() == len(NAMES)


@pytest.mark.usefixtures("needs_insert_returning")
async def test_upsert_does_not_resurrect_a_deleted_row(
    soft_users: SoftUserRepository,
):
    created = await soft_users.create_or_update(name="John")
    await soft_users.remove(SoftUser.id == created.id)
    await soft_users.create_or_update(id=created.id, name="John")
    assert await soft_users.count() == 0


@pytest.mark.usefixtures("needs_insert_returning")
async def test_get_or_create_refuses_to_reuse_a_deleted_row():
    repository = SoftUserByNameRepository()
    created = await repository.get_or_create(name="John")
    await repository.remove(SoftUser.id == created.id)
    with pytest.raises(DeletedRowExistsError):
        await repository.get_or_create(name="John")
