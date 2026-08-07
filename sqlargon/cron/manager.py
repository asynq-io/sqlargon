from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from functools import partial
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, TypeGuard, TypeVar

import anyio
from anyio import TASK_STATUS_IGNORED, CapacityLimiter, to_thread

from sqlargon.utils import utc_now

from .repository import CronTaskRepository
from .utils import next_run_time, validate_schedule

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from anyio.abc import TaskGroup, TaskStatus

    from .models import CronTask


logger = logging.getLogger(__name__)

# A task function, either sync or async
TaskFunc = Callable[..., "Awaitable[None] | None"]

F = TypeVar("F", bound=TaskFunc)


def _default_name(func: TaskFunc) -> str:
    """The task name of ``func`` when none is given explicitly."""
    return getattr(func, "__name__", None) or type(func).__name__


def _is_async(func: TaskFunc) -> TypeGuard[Callable[..., Awaitable[object]]]:
    """Whether calling ``func`` returns an awaitable.

    Callable objects are classified by their ``__call__``, so an instance with
    an async ``__call__`` is not mistaken for a sync function.
    """
    return iscoroutinefunction(func) or iscoroutinefunction(
        getattr(func, "__call__", None)  # noqa: B004
    )


def _in_thread(func: TaskFunc) -> Callable[..., Awaitable[object]]:
    """Wrap a sync task function to run in a worker thread."""

    def call(*args: Any, **kwargs: Any) -> Awaitable[object]:
        return to_thread.run_sync(partial(func, *args, **kwargs))

    return call


@dataclass(frozen=True, slots=True)
class _Registration:
    """A function a scheduler can execute, awaitable by the time it is stored.

    A ``schedule`` makes it declarative, i.e. reconciled by :meth:`Cron.sync`.
    """

    func: Callable[..., Awaitable[object]]
    schedule: str | None = None


class Cron:
    """Database-backed cron scheduler for a single namespace.

    Declarative mode: decorate functions with :meth:`task` and call
    :meth:`run` (or :meth:`sync`); declared tasks are created or updated and
    tasks no longer declared are deleted. Imperative mode: manage schedules
    at runtime with :meth:`schedule` and :meth:`unschedule`.

    Multiple instances may run the same namespace concurrently; due tasks
    are claimed with ``FOR UPDATE SKIP LOCKED`` so each run executes once.
    """

    def __init__(
        self,
        namespace: str = "default",
        *,
        repository: CronTaskRepository | None = None,
        poll_interval: float = 1.0,
        max_concurrency: int = 32,
        batch_size: int = 100,
    ) -> None:
        self.namespace = namespace
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        # a repository carries the statement it is building, so each scheduler
        # needs its own; pass one bound elsewhere with
        # ``CronTaskRepository().using(db=...)`` to use another database
        self.repository = repository if repository is not None else CronTaskRepository()
        self.max_concurrency = max_concurrency
        self._registry: dict[str, _Registration] = {}
        # the reverse index of the registry, keyed by the function as given
        self._names: dict[Any, str] = {}
        self._limiter: CapacityLimiter | None = None

    @property
    def limiter(self) -> CapacityLimiter:
        """The bound on concurrent executions.

        Built on first execution rather than in ``__init__``, so a scheduler
        can be constructed at import time, before an event loop exists.
        """
        if self._limiter is None:
            self._limiter = CapacityLimiter(self.max_concurrency)
        return self._limiter

    def task(
        self, schedule: str | None = None, *, name: str | None = None
    ) -> Callable[[F], F]:
        """Register the decorated function; with ``schedule`` it becomes a
        declarative task reconciled by :meth:`sync`."""

        def decorator(func: F) -> F:
            task_name = self.register(func, name=name)
            if schedule is not None:
                self._registry[task_name] = replace(
                    self._registry[task_name], schedule=validate_schedule(schedule)
                )
            return func

        return decorator

    def register(self, func: TaskFunc, *, name: str | None = None) -> str:
        """Make ``func`` executable by this scheduler and return its name.

        Sync functions are wrapped once here to run in a worker thread. A name
        registered again keeps the schedule it was declared with.
        """
        name = name or _default_name(func)
        declared = self._registry.get(name)
        self._registry[name] = _Registration(
            func if _is_async(func) else _in_thread(func),
            declared.schedule if declared else None,
        )
        self._names[func] = name
        return name

    async def sync(self) -> None:
        """Reconcile declarative tasks with the database: create missing
        ones, update changed schedules and delete undeclared ones.

        A declared name already held by an imperative row takes that row over
        rather than leaving the declaration unapplied.
        """
        declared = {
            name: registration.schedule
            for name, registration in self._registry.items()
            if registration.schedule is not None
        }
        await self.repository.reconcile(self.namespace, declared, utc_now())

    async def schedule(
        self,
        func: TaskFunc | str,
        schedule: str,
        *args: Any,
        name: str | None = None,
        **kwargs: Any,
    ) -> CronTask:
        """Create or update a task at runtime.

        ``func`` may be a callable (registered automatically) or the name of
        a function registered on another instance of this namespace. Extra
        ``args`` and ``kwargs`` must be JSON-serializable; they are stored
        with the task and passed to the function on execution.
        """
        name = self.register(func, name=name) if callable(func) else name or func
        values = {
            "namespace": self.namespace,
            "name": name,
            "schedule": validate_schedule(schedule),
            "next_run_at": next_run_time(schedule, utc_now()),
            "args": list(args) or None,
            "kwargs": kwargs or None,
        }
        return await self.repository.upsert(
            values,
            return_results=True,
            index_elements={"namespace", "name"},
            set_={"schedule", "next_run_at", "args", "kwargs"},
        ).one()

    async def unschedule(self, func: TaskFunc | str) -> None:
        """Delete the task from the database."""
        await self.repository.remove(
            namespace=self.namespace, name=self._task_name(func)
        )

    async def pause(self, func: TaskFunc | str) -> None:
        """Stop scheduling the task without deleting it."""
        await self._set_enabled(self._task_name(func), enabled=False)

    async def resume(self, func: TaskFunc | str) -> None:
        """Re-enable a paused task."""
        await self._set_enabled(self._task_name(func), enabled=True)

    async def tasks(self) -> Sequence[CronTask]:
        """Return all tasks stored in this namespace."""
        return await self.repository.list(namespace=self.namespace)

    async def run(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        """Sync declarative tasks, then poll and execute due ones until
        cancelled.

        Polling errors are logged and retried on the next tick, so a database
        blip never takes the scheduler down.
        """
        await self.sync()
        async with anyio.create_task_group() as tg:
            task_status.started()
            while True:
                await self._poll(tg)
                await anyio.sleep(self.poll_interval)

    @asynccontextmanager
    async def running(self) -> AsyncGenerator[Cron]:
        """Run the scheduler in the background, e.g. in an ASGI lifespan."""
        async with anyio.create_task_group() as tg:
            await tg.start(self.run)
            try:
                yield self
            finally:
                tg.cancel_scope.cancel()

    def _task_name(self, func: TaskFunc | str) -> str:
        """Resolve ``func`` to the name it was registered under."""
        if isinstance(func, str):
            return func
        return self._names.get(func) or _default_name(func)

    async def _poll(self, tg: TaskGroup) -> None:
        try:
            tasks = await self.repository.claim_due(
                self.namespace,
                utc_now(),
                names=set(self._registry),
                limit=self.batch_size,
            )
        except Exception:
            logger.exception("Claiming due cron tasks failed")
            return
        for task in tasks:
            tg.start_soon(self._execute, task)

    async def _set_enabled(self, name: str, *, enabled: bool) -> None:
        await self.repository.update_one(
            {"enabled": enabled}, namespace=self.namespace, name=name
        )

    async def _execute(self, task: CronTask) -> None:
        registration = self._registry.get(task.name)
        if registration is None:
            logger.warning("No function registered for cron task %r", task.name)
            return
        async with self.limiter:
            try:
                await registration.func(*(task.args or ()), **(task.kwargs or {}))
            except Exception:
                logger.exception("Cron task %r failed", task.name)
