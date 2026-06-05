from __future__ import annotations

from sqlakeyset import unserialize_bookmark
from sqlakeyset.paging import (
    core_page_from_rows,
    prepare_paging,
)

from .abc import PaginationStrategy
from .models import CursorPage


class CursorPaginationStrategy(PaginationStrategy[[str | None, int], CursorPage]):
    async def paginate(
        self,
        cursor: str | None,
        page_size: int,
    ) -> CursorPage:
        place, backwards = (None, False)
        if cursor:
            place, backwards = unserialize_bookmark(cursor)
        async with self.session() as session:
            sel = prepare_paging(
                q=self.query,
                per_page=page_size,
                place=place,
                backwards=backwards,
                orm=False,
                dialect=session.bind.dialect,
            )
            selected = await session.execute(sel.select)
        keys = list(selected.keys())
        idx = len(keys) - len(sel.extra_columns)
        keys = keys[:idx]
        page_result = core_page_from_rows(
            sel,
            selected.fetchall(),
            keys,
            None,
            page_size,
            backwards,
            current_place=place,
        )
        as_model = self.pagination_options.get("as_model", True)
        items = [p[0] if as_model else p._mapping for p in page_result]  # noqa: SLF001

        return CursorPage(
            items=items,
            current_page=cursor,
            next_page=page_result.paging.bookmark_next
            if page_result.paging.has_next
            else None,
            previous_page=page_result.paging.bookmark_previous
            if page_result.paging.has_previous
            else None,
        )
