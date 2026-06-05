from __future__ import annotations

from .abc import BaseOffsetLimitPaginationStrategy, BaseTotalPaginationStrategy
from .models import NumberedPage, TotalNumberedPage


class NumberedPaginationStrategy(
    BaseOffsetLimitPaginationStrategy[[int, int], NumberedPage]
):
    async def paginate(
        self,
        page: int,
        page_size: int,
    ) -> NumberedPage:
        offset = (page - 1) * page_size
        items = await self._get_offset_limit_page(offset, page_size)
        return NumberedPage(
            items=items,
            current_page=page,
            page_size=page_size,
            items_on_page=len(items),
        )


class TotalNumberedPaginationStrategy(
    BaseTotalPaginationStrategy[[int, int], TotalNumberedPage]
):
    async def paginate(
        self,
        page: int,
        page_size: int,
    ) -> TotalNumberedPage:
        offset = (page - 1) * page_size
        items, total_records = await self._get_items_with_total(offset, page_size)
        total_pages = (total_records + page_size - 1) // page_size

        return TotalNumberedPage(
            items=items,
            current_page=page,
            page_size=page_size,
            items_on_page=len(items),
            total_pages=total_pages,
            total_items=total_records,
        )
