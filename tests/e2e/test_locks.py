import anyio
import pytest

from sqlargon import Database
from sqlargon.query_builder import Option, UnsupportedOption

from .backends import Backend

HOLD = 0.2


async def hold(database: Database, name: str, order: list[str]) -> None:
    """Take the lock, mark entry and exit, and hold it in between."""
    async with database.lock("e2e"):
        order.append(f"{name}-in")
        await anyio.sleep(HOLD)
        order.append(f"{name}-out")


def is_serialised(order: list[str]) -> bool:
    """Whether no holder entered the lock while another still held it."""
    return all(
        order[index].removesuffix("-in") == order[index + 1].removesuffix("-out")
        for index in range(0, len(order), 2)
    )


async def test_lock_serialises_tasks_on_one_database(db: Database):
    order: list[str] = []
    async with anyio.create_task_group() as group:
        group.start_soon(hold, db, "a", order)
        group.start_soon(hold, db, "b", order)
    assert len(order) == 4
    assert is_serialised(order)


@pytest.mark.usefixtures("needs_native_locks")
async def test_advisory_lock_serialises_two_databases(database_url: str):
    """A native lock is held on the server, so it spans engines."""
    first, second = Database(database_url), Database(database_url)
    order: list[str] = []
    try:
        async with anyio.create_task_group() as group:
            group.start_soon(hold, first, "a", order)
            group.start_soon(hold, second, "b", order)
    finally:
        await first.dispose()
        await second.dispose()
    assert len(order) == 4
    assert is_serialised(order)


@pytest.mark.usefixtures("needs_native_locks")
async def test_advisory_lock_is_released_after_the_block(db: Database):
    async with db.lock("e2e"):
        pass
    with anyio.fail_after(HOLD * 10):
        async with db.lock("e2e"):
            pass


@pytest.mark.usefixtures("needs_native_locks")
async def test_advisory_lock_is_released_on_error(db: Database):
    with pytest.raises(RuntimeError):
        async with db.lock("e2e"):
            raise RuntimeError
    with anyio.fail_after(HOLD * 10):
        async with db.lock("e2e"):
            pass


async def test_distinct_lock_names_do_not_block_each_other(db: Database):
    with anyio.fail_after(HOLD * 10):
        async with db.lock("first"), db.lock("second"):
            pass


async def test_native_lock_availability_matches_the_backend(
    db: Database, backend: Backend
):
    if backend.native_locks:
        async with db.native_lock("e2e"):
            pass
    else:
        assert not db.query_builder.supports(Option.LOCKS)
        with pytest.raises(UnsupportedOption):
            db.query_builder.get_lock_pair("e2e")
