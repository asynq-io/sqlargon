from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, Union

from databases.interfaces import Record
from sqlalchemy import (
    Select,
    func,
)

from .orm import Model

P = TypeVar("P", bound=Union[str, int, None])

if TYPE_CHECKING:
    from .database import Database


@dataclass
class BasePage(Generic[Model]):
    items: Sequence[Model] | Sequence[Record]


@dataclass
class TokenPage(BasePage[Model]):
    current_page: str | None
    next_page: str | None
    previous_page: str | None


@dataclass
class NumberedPage(BasePage[Model]):
    current_page: int
    page_size: int
    total_pages: int | None
    total_items: int | None


class PaginationStrategy(Generic[P, Model], ABC):
    def __init__(self, db: Database, query: Select) -> None:
        self.db = db
        self.query = query

    def _convert_to_models(self, page_result: Any) -> list[Model]:
        return [p[0] for p in page_result]

    @abstractmethod
    async def paginate(
        self,
        page: P,
        page_size: int = 100,
        **kwargs,
    ) -> BasePage:
        raise NotImplementedError


# class TokenPaginationStrategy(PaginationStrategy[Union[str, None], Model]):
#     async def paginate(
#         self,
#         page: str | None = None,
#         page_size: int = 100,
#         as_model: bool = True,
#         **kwargs,
#     ) -> BasePage:
#         place, backwards = unserialize_bookmark(page)  # type: ignore
#         sel = prepare_paging(
#             q=self.query,
#             per_page=page_size,
#             place=place,
#             backwards=backwards,
#             orm=False,
#             dialect=self.db.dialect,
#         )
#         selected = await self.db.fetch_all(sel.select)
#         keys = list(selected[0].keys())  # type: ignore[attr-defined]
#         idx = len(keys) - len(sel.extra_columns)
#         keys = keys[:idx]
# page_result = core_page_from_rows(
#     sel,
#     [s._row for s in selected],
#     keys,
#     None,
#     page_size,
#     backwards,
#     current_place=place,
# )
# paging = Paging(selected, page_size, backwards, place, )
# return TokenPage(
#     items=selected,
#     current_page=page,
#     next_page=page_result.paging.bookmark_next
#     if page_result.paging.has_next
#     else None,
#     previous_page=page_result.paging.bookmark_previous
#     if page_result.paging.has_previous
#     else None,
# )


class NumberedPaginationStrategy(PaginationStrategy[int, Model]):
    async def paginate(
        self,
        page: int = 1,
        page_size: int = 100,
        include_total: bool = True,
        **kwargs,
    ) -> BasePage:
        offset = (page - 1) * page_size

        page_query = self.query.offset(offset).limit(page_size)

        if include_total:
            total_records_query = Select(func.count()).select_from(
                self.query.subquery()
            )
            async with self.db.transaction():
                total_records = await self.db.fetch_val(total_records_query)
                page_result = await self.db.fetch_all(page_query)
            total_pages = (total_records + page_size - 1) // page_size
        else:
            total_records = None
            total_pages = None
            page_result = await self.db.fetch_all(page_query)

        return NumberedPage(
            items=page_result,
            current_page=page,
            page_size=len(page_result),
            total_pages=total_pages,
            total_items=total_records,
        )
