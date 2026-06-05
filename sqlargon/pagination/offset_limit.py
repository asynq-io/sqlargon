from __future__ import annotations

from .abc import BaseOffsetLimitPaginationStrategy, BaseTotalPaginationStrategy
from .models import BasePage, TotalBasePage


class OffsetLimitPaginationStrategy(
    BaseOffsetLimitPaginationStrategy[[int, int], BasePage]
):
    async def paginate(
        self,
        offset: int,
        limit: int,
    ) -> BasePage:
        items = await self._get_offset_limit_page(offset, limit)
        return BasePage(items=items)


class TotalOffsetLimitPaginationStrategy(
    BaseTotalPaginationStrategy[[int, int], TotalBasePage]
):
    async def paginate(self, offset: int, limit: int) -> TotalBasePage:
        items, total_records = await self._get_items_with_total(offset, limit)
        return TotalBasePage(items=items, total_items=total_records)
