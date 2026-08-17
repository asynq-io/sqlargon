# Cron

`sqlargon.cron` is a database-backed scheduler: cron tasks are stored as
rows, and one or more app instances poll for due ones and execute them.
Because due tasks are claimed with `SELECT ... FOR UPDATE SKIP LOCKED`,
running several instances of the same app is safe — each run is executed by
exactly one instance. It requires the `sqlargon[cron]` extra (`croniter` for
schedule arithmetic, `anyio` for concurrency).

```python
from sqlargon.cron import Cron

cron = Cron(namespace="my-app")


@cron.task("*/5 * * * *")
async def cleanup() -> None: ...


await cron.run()  # runs until cancelled
```

Every task lives in a *namespace*, so multiple apps (or multiple schedulers
within one app) can share a database without seeing each other's tasks.
Cron expressions are validated eagerly — an invalid one raises `ValueError`
at decoration or scheduling time, not when the row is first polled.

## Declarative tasks

Decorate functions with a schedule and let the scheduler reconcile the
database on startup. `sync()` (called automatically by `run()`) creates
missing tasks, recomputes `next_run_at` for changed schedules, and deletes
declarative tasks that are no longer declared — renaming or removing a
decorated function cleans up its row on the next start.

```python
@cron.task("0 8 * * MON", name="weekly-report")
async def send_report() -> None: ...
```

`task()` also takes the function outright, for one that is not yours to
decorate — a method of an object built elsewhere, say:

```python
cron.task("0 3 * * *", "purge_outbox", relay.purge)
```

Sync only ever deletes rows it created (they are marked as declarative), so
imperative tasks in the same namespace are never deleted. Declaring a name
that an imperative row already holds takes that row over — the declaration is
applied and the row becomes declarative.

Reconciliation is two statements whatever the number of tasks: one upsert
returning every declared row, then one delete of the declarative rows it did
not return. A task whose schedule is unchanged keeps its `next_run_at`, so a
run that came due while the scheduler was down is not skipped by a restart.

## Imperative tasks

Schedules can also be managed at runtime. Extra positional and keyword
arguments are stored with the task (in JSON columns) and passed to the
function on execution — they must be JSON-serializable:

```python
await cron.schedule(notify, "0 * * * *", 42, channel="sms")

await cron.pause("notify")      # keep the row, stop scheduling
await cron.resume("notify")
await cron.unschedule("notify")  # delete the row

for task in await cron.tasks():
    print(task.name, task.schedule, task.next_run_at)
```

`schedule()` upserts on `(namespace, name)`, so calling it again updates the
schedule and arguments in place. The first argument may be the callable
itself (registered automatically) or the name of a function registered
elsewhere — e.g. decorated with `@cron.task()` (no schedule) or passed to
`cron.register()` on the instances that should execute it:

```python
@cron.task()  # registered, but scheduled imperatively
async def notify(user_id: int, *, channel: str = "email") -> None: ...


await cron.schedule("notify", "0 * * * *", 42)
```

## Running the scheduler

`run()` syncs declarative tasks and then polls until cancelled. Use it
directly as a long-running entrypoint, or start it in the background:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with cron.running():  # yields once the scheduler is live
        yield


app = FastAPI(lifespan=lifespan)
```

`run()` also supports anyio's initialization barrier, so it can be embedded
in an existing task group with `await tg.start(cron.run)`.

Execution semantics:

- Sync functions are registered once as thread-wrapped callables and run in
  a worker thread; async functions run on the event loop.
- Concurrent executions are bounded by a `CapacityLimiter`
  (`max_concurrency`); claims beyond the limit wait for a free slot.
- A failing task is logged (logger `sqlargon.cron.manager`) and retried at
  its next scheduled time; it never crashes the scheduler.
- Only tasks whose function is registered on the polling instance are
  claimed, so a task registered on a subset of the instances is executed
  there instead of having its run consumed and dropped elsewhere. A task no
  instance has registered stays due until one registers it.
- A failing poll (a dropped connection, say) is logged and retried on the
  next tick; the scheduler keeps running.

## Multiple instances

A claim locks the due rows with `FOR UPDATE SKIP LOCKED` and advances their
`next_run_at` within the same transaction. Instances polling concurrently
skip locked rows, and once the claim commits, the task is no longer due —
each run fires exactly once per schedule slot, and the row lock is held only
for the claim, never during execution.

## Configuration

`Cron` takes:

- `namespace` — the namespace this scheduler owns (default `"default"`).
- `repository` — the `CronTaskRepository` to store tasks with; defaults to a
  fresh one bound to the process-wide default database. Pass
  `CronTaskRepository().using(db=other_database)` to run a scheduler against
  another database.
- `poll_interval` — seconds between polls (default `1.0`).
- `max_concurrency` — bound on concurrent task executions (default `32`).
- `batch_size` — maximum rows claimed per poll (default `100`).

## The cron_tasks table

Tasks are stored in the `cron_tasks` table, registered on the shared `Base`
metadata when `sqlargon.cron` is imported — `db.create_all()` creates it,
and an Alembic autogenerate pass picks it up (see
[Alembic Migrations](migrations.md)).
