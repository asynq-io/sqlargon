from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlargon.repository import SQLAlchemyRepository

from .models import CronTask
from .utils import next_run_time

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence
    from datetime import datetime


# the unique key of CronTask, i.e. what an upsert of a declared task conflicts on
CONFLICT_KEY = {"namespace", "name"}


class CronTaskRepository(SQLAlchemyRepository[CronTask]):
    """Repository for :class:`CronTask` rows."""

    async def reconcile(
        self, namespace: str, schedules: Mapping[str, str], now: datetime
    ) -> None:
        """Make the declarative tasks of ``namespace`` exactly ``schedules``.

        The declared tasks are upserted -- creating the missing ones and taking
        over any row already holding their name -- and one delete removes the
        declarative rows no longer declared. Rows scheduled imperatively are
        left alone.

        ``next_run_at`` is only recomputed for a task whose schedule changed,
        so a run that came due while the scheduler was down still fires.
        """
        async with self.session():
            if schedules:
                await self._upsert_declared(namespace, schedules, now)
            await self.remove(
                CronTask.namespace == namespace,
                CronTask.declarative,
                CronTask.name.not_in(list(schedules)),
            )

    async def _upsert_declared(
        self, namespace: str, schedules: Mapping[str, str], now: datetime
    ) -> None:
        """Create or update the declared tasks of ``namespace``.

        The tasks whose schedule changed are upserted apart from the rest, so
        that only they have their ``next_run_at`` overwritten. Which group a
        task belongs to is decided here, from the schedules read back first,
        rather than by a conditional assignment in the upsert itself: MySQL
        applies the assignments of an ``ON DUPLICATE KEY UPDATE`` in order, so
        one reading ``schedule`` would already see the value the same statement
        had just written to it.
        """
        stored = await self._stored_schedules(namespace, schedules)
        unchanged: list[dict[str, Any]] = []
        rescheduled: list[dict[str, Any]] = []
        for name, schedule in schedules.items():
            group = unchanged if stored.get(name) == schedule else rescheduled
            group.append(
                {
                    "namespace": namespace,
                    "name": name,
                    "schedule": schedule,
                    "declarative": True,
                    "next_run_at": next_run_time(schedule, now),
                }
            )
        if unchanged:
            await self.bulk_create_or_update(
                unchanged,
                index_elements=CONFLICT_KEY,
                set_={"schedule", "declarative"},
            )
        if rescheduled:
            await self.bulk_create_or_update(
                rescheduled,
                index_elements=CONFLICT_KEY,
                set_={"schedule", "declarative", "next_run_at"},
            )

    async def _stored_schedules(
        self, namespace: str, names: Collection[str]
    ) -> dict[str, str]:
        """The schedule each of ``names`` currently holds in ``namespace``.

        Only the two columns are read, so the rows the upsert is about to
        overwrite are not left stale in the session identity map.
        """
        query = self.select(CronTask.name, CronTask.schedule).filter(
            CronTask.namespace == namespace, CronTask.name.in_(list(names))
        )
        return {row["name"]: row["schedule"] for row in await query.mappings()}

    async def claim_due(
        self,
        namespace: str,
        now: datetime,
        *,
        names: Collection[str] | None = None,
        limit: int = 100,
    ) -> Sequence[CronTask]:
        """Claim tasks due at ``now`` and return them.

        Due rows are locked with ``FOR UPDATE SKIP LOCKED`` and their
        ``next_run_at`` advanced within the same transaction, so concurrent
        instances polling the same namespace never claim the same run.

        With ``names``, only those tasks are claimed -- a caller that cannot
        execute a task leaves its run to an instance that can, instead of
        consuming the slot and dropping it.
        """
        filters = [
            CronTask.namespace == namespace,
            CronTask.enabled,
            CronTask.next_run_at <= now,
        ]
        if names is not None:
            filters.append(CronTask.name.in_(names))
        async with self.session():
            tasks = await (
                self.select(with_for_update={"skip_locked": True})
                .filter(*filters)
                .order_by(CronTask.next_run_at)
                .limit(limit)
                .all()
            )
            for task in tasks:
                task.last_run_at = now
                task.next_run_at = next_run_time(task.schedule, now)
            return tasks
