import logging
from datetime import timedelta, timezone

import anyio
import pytest

from sqlargon import Database
from sqlargon.cron import Cron, CronTask
from sqlargon.cron.utils import next_run_time, validate_schedule
from sqlargon.utils import utc_now

EVERY_MINUTE = "* * * * *"


@pytest.fixture
async def cron(db: Database):
    async with db.engine.begin() as conn:
        await conn.run_sync(CronTask.__table__.create, checkfirst=True)
    yield Cron(namespace="test", poll_interval=0.05)
    async with db.engine.begin() as conn:
        await conn.run_sync(CronTask.__table__.drop, checkfirst=True)


def test_validate_schedule():
    assert validate_schedule(EVERY_MINUTE) == EVERY_MINUTE


def test_validate_schedule_invalid():
    with pytest.raises(ValueError, match="Invalid cron expression"):
        validate_schedule("not a cron")


def test_next_run_time():
    now = utc_now()
    next_run = next_run_time(EVERY_MINUTE, now)
    assert next_run > now
    assert next_run - now <= timedelta(minutes=1)
    assert next_run.tzinfo == timezone.utc


async def test_sync_creates_declared_tasks(cron):
    @cron.task("*/5 * * * *")
    async def cleanup() -> None: ...

    @cron.task(EVERY_MINUTE, name="renamed")
    async def other() -> None: ...

    await cron.sync()
    tasks = {task.name: task for task in await cron.tasks()}
    assert set(tasks) == {"cleanup", "renamed"}
    assert tasks["cleanup"].schedule == "*/5 * * * *"
    assert tasks["cleanup"].declarative is True
    assert tasks["cleanup"].enabled is True
    assert tasks["cleanup"].next_run_at > utc_now()


async def test_sync_updates_and_deletes(cron):
    @cron.task("*/5 * * * *")
    async def cleanup() -> None: ...

    await cron.sync()
    old_task = (await cron.tasks())[0]

    other = Cron(namespace="test")

    @other.task("0 0 * * *", name="cleanup")
    async def new_cleanup() -> None: ...

    await other.sync()
    task = (await other.tasks())[0]
    assert task.schedule == "0 0 * * *"
    assert task.next_run_at != old_task.next_run_at

    empty = Cron(namespace="test")
    await empty.sync()
    assert await empty.tasks() == []


async def test_sync_keeps_imperative_tasks(cron):
    async def report() -> None: ...

    await cron.schedule(report, EVERY_MINUTE)
    await cron.sync()
    tasks = await cron.tasks()
    assert len(tasks) == 1
    assert tasks[0].declarative is False


async def test_sync_takes_over_an_imperative_row(cron):
    async def report() -> None: ...

    await cron.schedule(report, EVERY_MINUTE)

    declaring = Cron(namespace="test")

    @declaring.task("0 0 * * *", name="report")
    async def declared() -> None: ...

    await declaring.sync()
    tasks = await declaring.tasks()
    assert len(tasks) == 1
    assert tasks[0].schedule == "0 0 * * *"
    assert tasks[0].declarative is True


async def test_sync_reapplies_a_declaration_rescheduled_at_runtime(cron):
    @cron.task("0 0 * * *")
    async def job() -> None: ...

    await cron.schedule(job, EVERY_MINUTE)

    await cron.sync()

    task = (await cron.tasks())[0]
    assert task.schedule == "0 0 * * *"
    assert task.declarative is True


async def test_sync_keeps_a_due_run_due(cron):
    @cron.task(EVERY_MINUTE)
    async def job() -> None: ...

    await cron.sync()
    due = utc_now() - timedelta(seconds=1)
    await cron.repository.update_one({"next_run_at": due}, namespace="test", name="job")

    await cron.sync()

    assert (await cron.tasks())[0].next_run_at == due


async def test_sync_leaves_a_paused_task_paused(cron):
    @cron.task(EVERY_MINUTE)
    async def job() -> None: ...

    await cron.sync()
    await cron.pause(job)

    await cron.sync()

    assert (await cron.tasks())[0].enabled is False


async def test_schedule_and_unschedule(cron):
    async def report() -> None: ...

    task = await cron.schedule(report, "0 8 * * 1")
    assert task.name == "report"
    assert task.schedule == "0 8 * * 1"

    task = await cron.schedule("report", "0 9 * * 1")
    assert task.schedule == "0 9 * * 1"
    assert len(await cron.tasks()) == 1

    await cron.unschedule(report)
    assert await cron.tasks() == []


async def test_schedule_invalid_expression(cron):
    async def report() -> None: ...

    with pytest.raises(ValueError, match="Invalid cron expression"):
        await cron.schedule(report, "invalid")


async def test_namespace_isolation(cron):
    other = Cron(namespace="other")

    async def job() -> None: ...

    await cron.schedule(job, EVERY_MINUTE)
    await other.schedule(job, EVERY_MINUTE)

    await cron.unschedule("job")
    assert await cron.tasks() == []
    assert len(await other.tasks()) == 1


async def test_claim_due(cron):
    async def job() -> None: ...

    await cron.schedule(job, EVERY_MINUTE)
    now = utc_now() + timedelta(minutes=2)

    claimed = await cron.repository.claim_due("test", now)
    assert [task.name for task in claimed] == ["job"]
    # the same run cannot be claimed twice
    assert list(await cron.repository.claim_due("test", now)) == []

    task = (await cron.tasks())[0]
    assert task.last_run_at == now
    assert task.next_run_at > now


async def test_claim_due_only_claims_the_given_names(cron):
    async def job() -> None: ...

    async def ghost() -> None: ...

    await cron.schedule(job, EVERY_MINUTE)
    await cron.schedule(ghost, EVERY_MINUTE)
    now = utc_now() + timedelta(minutes=2)

    claimed = await cron.repository.claim_due("test", now, names={"job"})

    assert [task.name for task in claimed] == ["job"]
    untouched = await cron.repository.get(namespace="test", name="ghost")
    assert untouched.last_run_at is None
    assert untouched.next_run_at < now


async def test_claim_due_skips_other_namespaces(cron):
    async def job() -> None: ...

    await cron.schedule(job, EVERY_MINUTE)
    now = utc_now() + timedelta(minutes=2)
    assert list(await cron.repository.claim_due("other", now)) == []


@pytest.mark.parametrize("action", ["pause", "resume", "unschedule"])
async def test_task_actions_accept_a_renamed_callable(cron, action):
    @cron.task(EVERY_MINUTE, name="renamed")
    async def job() -> None: ...

    await cron.sync()

    await getattr(cron, action)(job)

    tasks = await cron.tasks()
    if action == "unschedule":
        assert tasks == []
    else:
        assert tasks[0].enabled is (action == "resume")


async def test_pause_and_resume(cron):
    async def job() -> None: ...

    await cron.schedule(job, EVERY_MINUTE)
    await cron.pause(job)
    now = utc_now() + timedelta(minutes=2)
    assert list(await cron.repository.claim_due("test", now)) == []

    await cron.resume("job")
    claimed = await cron.repository.claim_due("test", now)
    assert [task.name for task in claimed] == ["job"]


async def test_run_executes_due_task(cron):
    ran = anyio.Event()

    @cron.task(EVERY_MINUTE)
    async def job() -> None:
        ran.set()

    await cron.sync()
    await cron.repository.update_one(
        {"next_run_at": utc_now() - timedelta(seconds=1)},
        namespace="test",
        name="job",
    )
    async with cron.running():
        with anyio.fail_after(2):
            await ran.wait()


async def test_run_executes_task_with_stored_args(cron):
    received = {}
    ran = anyio.Event()

    @cron.task()
    async def notify(user_id, *, channel="email") -> None:
        received["user_id"] = user_id
        received["channel"] = channel
        ran.set()

    task = await cron.schedule("notify", EVERY_MINUTE, 42, channel="sms")
    assert task.args == [42]
    assert task.kwargs == {"channel": "sms"}

    await cron.repository.update_one(
        {"next_run_at": utc_now() - timedelta(seconds=1)},
        namespace="test",
        name="notify",
    )
    async with cron.running():
        with anyio.fail_after(2):
            await ran.wait()
    assert received == {"user_id": 42, "channel": "sms"}


async def test_run_leaves_unregistered_tasks_due(cron):
    ran = anyio.Event()

    @cron.task(EVERY_MINUTE)
    async def job() -> None:
        ran.set()

    await cron.schedule("ghost", EVERY_MINUTE)
    await cron.sync()
    due = utc_now() - timedelta(seconds=1)
    await cron.repository.update_many({"next_run_at": due}, namespace="test")

    async with cron.running():
        with anyio.fail_after(2):
            await ran.wait()

    ghost = await cron.repository.get(namespace="test", name="ghost")
    assert ghost.next_run_at == due
    assert ghost.last_run_at is None


async def test_run_survives_a_failing_poll(cron, caplog, monkeypatch):
    ran = anyio.Event()
    claims = []

    @cron.task(EVERY_MINUTE)
    async def job() -> None:
        ran.set()

    await cron.sync()
    await cron.repository.update_one(
        {"next_run_at": utc_now() - timedelta(seconds=1)},
        namespace="test",
        name="job",
    )
    claim_due = cron.repository.claim_due

    async def flaky(*args, **kwargs):
        claims.append(1)
        if len(claims) == 1:
            msg = "connection lost"
            raise RuntimeError(msg)
        return await claim_due(*args, **kwargs)

    monkeypatch.setattr(cron.repository, "claim_due", flaky)

    with caplog.at_level(logging.ERROR, logger="sqlargon.cron.manager"):
        async with cron.running():
            with anyio.fail_after(2):
                await ran.wait()

    assert "Claiming due cron tasks failed" in caplog.text


def test_each_scheduler_gets_its_own_repository():
    assert Cron().repository is not Cron().repository


def test_limiter_is_built_on_first_use():
    scheduler = Cron(max_concurrency=4)

    assert scheduler._limiter is None
    assert scheduler.limiter.total_tokens == 4
    assert scheduler.limiter is scheduler._limiter


async def test_execute_callable_object_with_async_call(cron):
    class Job:
        def __init__(self) -> None:
            self.calls: list[int] = []

        async def __call__(self, value) -> None:
            self.calls.append(value)

    job = Job()
    name = cron.register(job)
    assert name == "Job"

    await cron._execute(CronTask(name=name, args=[1]))
    assert job.calls == [1]


async def test_execute_callable_object_with_sync_call(cron):
    class Job:
        def __init__(self) -> None:
            self.calls: list[int] = []

        def __call__(self) -> None:
            self.calls.append(1)

    job = Job()
    await cron._execute(CronTask(name=cron.register(job, name="job")))
    assert job.calls == [1]


async def test_execute_sync_function(cron):
    calls = []

    def job() -> None:
        calls.append(1)

    cron.register(job)
    await cron._execute(CronTask(name="job"))
    assert calls == [1]


async def test_execute_passes_args_and_kwargs(cron):
    calls = []

    def job(a, b=None) -> None:
        calls.append((a, b))

    cron.register(job)
    await cron._execute(CronTask(name="job", args=[1], kwargs={"b": 2}))
    assert calls == [(1, 2)]


async def test_execute_unregistered_function_warns(cron, caplog):
    with caplog.at_level(logging.WARNING, logger="sqlargon.cron.manager"):
        await cron._execute(CronTask(name="ghost"))
    assert "No function registered" in caplog.text


async def test_execute_logs_failures(cron, caplog):
    @cron.task(EVERY_MINUTE)
    async def boom() -> None:
        msg = "kaboom"
        raise RuntimeError(msg)

    with caplog.at_level(logging.ERROR, logger="sqlargon.cron.manager"):
        await cron._execute(CronTask(name="boom"))
    assert "failed" in caplog.text
