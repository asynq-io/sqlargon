from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa

from sqlargon.repository import SQLAlchemyRepository

from .models import CronTask
from .utils import next_run_time

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence
    from datetime import datetime
    from uuid import UUID


class CronTaskRepository(SQLAlchemyRepository[CronTask]):
    """Repository for :class:`CronTask` rows."""

    async def reconcile(
        self, namespace: str, schedules: Mapping[str, str], now: datetime
    ) -> None:
        """Make the declarative tasks of ``namespace`` exactly ``schedules``.

        One upsert creates every declared task and takes over any row already
        holding its name, and one delete removes the declarative rows the
        upsert did not return -- those no longer declared. Rows scheduled
        imperatively are left alone.

        ``next_run_at`` is only recomputed for a task whose schedule changed,
        so a run that came due while the scheduler was down still fires.
        """
        async with self.session():
            declared: list[UUID] = []
            if schedules:
                tasks = await self.bulk_create_or_update(
                    [
                        {
                            "namespace": namespace,
                            "name": name,
                            "schedule": schedule,
                            "declarative": True,
                            "next_run_at": next_run_time(schedule, now),
                        }
                        for name, schedule in schedules.items()
                    ],
                    return_results=True,
                    index_elements={"namespace", "name"},
                    set_=self._declarative_set(),
                )
                declared = [task.id for task in tasks]
            await self.remove(
                CronTask.namespace == namespace,
                CronTask.declarative,
                CronTask.id.not_in(declared),
            )

    def _declarative_set(self) -> dict[str, Any]:
        """The columns an upsert of a declared task overwrites."""
        excluded = self.qb.excluded(CronTask)
        return {
            "schedule": excluded.schedule,
            "declarative": excluded.declarative,
            "next_run_at": sa.case(
                (CronTask.schedule == excluded.schedule, CronTask.next_run_at),
                else_=excluded.next_run_at,
            ),
        }

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
