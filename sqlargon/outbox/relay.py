from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING

import anyio
from anyio import TASK_STATUS_IGNORED

from sqlargon.utils import utc_now

from .models import OutboxEvent
from .repository import OutboxEventRepository

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from uuid import UUID

    from anyio.abc import TaskStatus


logger = logging.getLogger(__name__)

# What a claimed event is handed to
Publisher = Callable[[OutboxEvent], Awaitable[None]]

ERROR_LENGTH = 1000


class OutboxRelay:
    """Publishes the events an :class:`.OutboxRepository` recorded.

    Claimed events are published one at a time, in the order they were
    written; an outbox that reorders its events is not much of an outbox. A
    publisher that raises stops the batch, so a transient broker outage
    delays the events behind it rather than overtaking them, and the failed
    event is retried with an exponential backoff until ``max_attempts``.

    Several relays may run against the same table: events are claimed with
    ``FOR UPDATE SKIP LOCKED`` and leased, so no two relays publish the same
    one::

        relay = OutboxRelay(publish)

        async with relay.running():
            ...

    Published events are kept as an audit trail; deleting the ones older than
    ``retention`` is :meth:`purge`, which the relay never runs on its own.
    """

    def __init__(  # noqa: PLR0913 -- keyword-only tuning knobs, each with a default
        self,
        publisher: Publisher,
        *,
        repository: OutboxEventRepository | None = None,
        poll_interval: float = 1.0,
        batch_size: int = 100,
        lease: timedelta = timedelta(minutes=5),
        max_attempts: int = 10,
        retry_backoff: float = 2.0,
        max_retry_delay: float = 300.0,
        retention: timedelta = timedelta(days=7),
    ) -> None:
        self.publisher = publisher
        # a repository carries the statement it is building, so each relay
        # needs its own; pass one bound elsewhere with
        # ``OutboxEventRepository().using(db=...)`` to use another database
        self.repository = (
            repository if repository is not None else OutboxEventRepository()
        )
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.lease = lease
        self.max_attempts = max_attempts
        self.retry_backoff = retry_backoff
        self.max_retry_delay = max_retry_delay
        self.retention = retention

    def retry_delay(self, attempts: int) -> timedelta:
        """How long to wait before retrying an event that failed ``attempts`` times."""
        delay = min(self.retry_backoff ** max(attempts - 1, 0), self.max_retry_delay)
        return timedelta(seconds=delay)

    async def dispatch_once(self) -> int:
        """Publish one batch of due events and return how many were published."""
        events = await self.repository.claim_pending(
            utc_now(),
            lease=self.lease,
            limit=self.batch_size,
            max_attempts=self.max_attempts,
        )
        published: list[UUID] = []
        for event in events:
            if not await self._publish(event):
                break
            published.append(event.id)
        await self.repository.mark_published(published, utc_now())
        return len(published)

    async def purge(self) -> int:
        """Delete the events published longer ago than ``retention``.

        The relay never purges on its own -- running it is up to the caller,
        and it takes no arguments so it can be registered as a cron task::

            cron.task("0 3 * * *", "purge_outbox", relay.purge)
        """
        return await self.repository.purge(utc_now() - self.retention)

    async def run(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        """Poll for due events and publish them until cancelled.

        Polling errors are logged and retried on the next tick, so a database
        blip never takes the relay down.
        """
        task_status.started()
        while True:
            await self._tick()
            await anyio.sleep(self.poll_interval)

    @asynccontextmanager
    async def running(self) -> AsyncGenerator[OutboxRelay]:
        """Run the relay in the background, e.g. in an ASGI lifespan."""
        async with anyio.create_task_group() as tg:
            await tg.start(self.run)
            try:
                yield self
            finally:
                tg.cancel_scope.cancel()

    async def _publish(self, event: OutboxEvent) -> bool:
        try:
            await self.publisher(event)
        except Exception as exc:
            logger.exception("Publishing outbox event %s failed", event.id)
            await self.repository.mark_failed(
                event.id,
                f"{type(exc).__name__}: {exc}"[:ERROR_LENGTH],
                utc_now() + self.retry_delay(event.attempts),
            )
            return False
        return True

    async def _tick(self) -> None:
        try:
            await self.dispatch_once()
        except Exception:
            logger.exception("Dispatching outbox events failed")
