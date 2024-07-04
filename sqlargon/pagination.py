from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from sqlakeyset import unserialize_bookmark
from sqlakeyset.paging import (
    core_page_from_rows,
    prepare_paging,
)
from sqlalchemy import (
    Executable,
    Row,
    Select,
    func,
)
from sqlalchemy.sql import ClauseElement

from . import Database, ORMModel

Model = TypeVar("Model", bound=ORMModel)


@dataclass
class BasePage(Generic[Model]):
    items: Sequence[Model] | Sequence[Row]


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


class PaginationStrategy(ABC):
    def _convert_to_models(self, page_result: Any) -> list[Model]:
        return [p[0] for p in page_result]

    @abstractmethod
    async def paginate(
        self,
        db: Database,
        query: Select | ClauseElement | Executable,
        as_model: bool = True,
        **kwargs,
    ) -> BasePage:
        pass


class TokenPaginationStrategy(PaginationStrategy):
    async def paginate(
        self,
        db: Database,
        query: Select | ClauseElement | Executable,
        as_model: bool = True,
        page: str | None = None,
        page_size: int = 100,
        **kwargs,
    ) -> BasePage:
        place, backwards = unserialize_bookmark(page)
        sel = prepare_paging(
            q=query,
            per_page=page_size,
            place=place,
            backwards=backwards,
            orm=False,
            dialect=db.engine.dialect,
        )
        selected = await db.execute(sel.select)
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
        return TokenPage(
            items=self._convert_to_models(page_result) if as_model else page_result,
            current_page=page,
            next_page=page_result.paging.bookmark_next
            if page_result.paging.has_next
            else None,
            previous_page=page_result.paging.bookmark_previous
            if page_result.paging.has_previous
            else None,
        )


class NumberedPaginationStrategy(PaginationStrategy):
    async def paginate(
        self,
        db: Database,
        query: Select | ClauseElement | Executable,
        as_model: bool = True,
        page_number: int = 1,
        page_size: int = 100,
        include_total: bool = True,
        **kwargs,
    ) -> BasePage:
        if include_total:
            total_records = (
                await db.execute(Select(func.count()).select_from(query.subquery()))
            ).scalar()
            total_pages = (total_records + page_size - 1) // page_size
        else:
            total_records = None
            total_pages = None

        offset = (page_number - 1) * page_size
        page_result = await db.execute(query.offset(offset).limit(page_size))

        return NumberedPage(
            items=self._convert_to_models(page_result) if as_model else page_result,
            current_page=page_number,
            page_size=page_size,
            total_pages=total_pages,
            total_items=total_records,
        )
