"""The database backed scheduler against a real server.

Reconciling declared tasks runs on every backend: it neither reads its rows
back with a RETURNING clause nor references the inserted row of an upsert, so
it needs nothing MySQL lacks -- see :mod:`tests.e2e.test_capabilities`.
"""

from datetime import timedelta
from typing import TYPE_CHECKING

import anyio
import pytest

from sqlargon import Database
from sqlargon.cron import Cron, CronTask, CronTaskRepository
from sqlargon.utils import utc_now

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

EVERY_MINUTE = "* * * * *"
NAMESPACE = "e2e"


@pytest.fixture
async def cron_table(db: Database) -> "AsyncGenerator[None]":
    async with db.engine.begin() as connection:
        await connection.run_sync(CronTask.__table__.create, checkfirst=True)
    try:
        yield
    finally:
        async with db.engine.begin() as connection:
            await connection.run_sync(CronTask.__table__.drop, checkfirst=True)


@pytest.fixture
def cron() -> Cron:
    return Cron(namespace=NAMESPACE, poll_interval=0.05)


@pytest.mark.usefixtures("cron_table")
async def test_sync_creates_the_declared_tasks(cron: Cron):
    @cron.task("*/5 * * * *")
    async def cleanup() -> None: ...

    await cron.sync()
    tasks = {task.name: task for task in await cron.tasks()}
    assert set(tasks) == {"cleanup"}
    assert tasks["cleanup"].next_run_at > utc_now()


@pytest.mark.usefixtures("cron_table")
async def test_sync_keeps_the_next_run_of_an_unchanged_schedule(cron: Cron):
    @cron.task(EVERY_MINUTE)
    async def cleanup() -> None: ...

    await cron.sync()
    first = (await cron.tasks())[0].next_run_at
    await cron.sync()
    assert (await cron.tasks())[0].next_run_at == first


@pytest.mark.usefixtures("cron_table")
async def test_sync_recomputes_only_the_changed_schedule(cron: Cron):
    """A sync mixing both groups, which is two upserts rather than one.

    The stale ``next_run_at`` of the rescheduled task is what a conditional
    assignment in the upsert would have failed to overwrite on MySQL.
    """

    @cron.task(EVERY_MINUTE)
    async def kept() -> None: ...

    @cron.task("0 0 * * *")
    async def changed() -> None: ...

    await cron.sync()
    before = {task.name: task.next_run_at for task in await cron.tasks()}

    rescheduling = Cron(namespace=NAMESPACE)
    rescheduling.task(EVERY_MINUTE, name="kept")(kept)
    rescheduling.task("*/5 * * * *", name="changed")(changed)
    await rescheduling.sync()

    after = {task.name: task for task in await rescheduling.tasks()}
    assert after["kept"].next_run_at == before["kept"]
    assert after["changed"].schedule == "*/5 * * * *"
    assert after["changed"].next_run_at < before["changed"]


@pytest.mark.usefixtures("cron_table")
async def test_sync_removes_a_task_no_longer_declared(cron: Cron):
    @cron.task(EVERY_MINUTE)
    async def cleanup() -> None: ...

    await cron.sync()
    assert len(await cron.tasks()) == 1

    fresh = Cron(namespace=NAMESPACE)
    await fresh.sync()
    assert await fresh.tasks() == []


@pytest.mark.usefixtures("cron_table")
async def test_run_executes_a_due_task(cron: Cron):
    ran = anyio.Event()

    @cron.task(EVERY_MINUTE)
    async def cleanup() -> None:
        ran.set()

    await cron.sync()
    await cron.repository.update({"next_run_at": utc_now()}).execute()

    with anyio.move_on_after(5):
        async with anyio.create_task_group() as group:
            group.start_soon(cron.run)
            await ran.wait()
            group.cancel_scope.cancel()

    assert ran.is_set()


@pytest.mark.usefixtures("cron_table", "needs_skip_locked")
async def test_concurrent_claims_never_overlap(database_url: str, cron: Cron):
    @cron.task(EVERY_MINUTE)
    async def cleanup() -> None: ...

    await cron.sync()
    due = utc_now() + timedelta(minutes=1)

    claimed: list[Sequence[CronTask]] = []

    async def claim(database: Database) -> None:
        repository = CronTaskRepository().using(db=database)
        claimed.append(await repository.claim_due(NAMESPACE, due))

    first, second = Database(database_url), Database(database_url)
    try:
        async with anyio.create_task_group() as group:
            group.start_soon(claim, first)
            group.start_soon(claim, second)
    finally:
        await first.dispose()
        await second.dispose()

    assert sorted(len(batch) for batch in claimed) == [0, 1]


@pytest.mark.usefixtures("cron_table")
async def test_claim_advances_the_next_run(cron: Cron):
    @cron.task(EVERY_MINUTE)
    async def cleanup() -> None: ...

    await cron.sync()
    due = utc_now() + timedelta(minutes=1)
    claimed = await cron.repository.claim_due(NAMESPACE, due)
    assert len(claimed) == 1

    task = (await cron.tasks())[0]
    assert task.last_run_at == due
    assert task.next_run_at > due
