from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar, Union

from sqlakeyset import unserialize_bookmark
from sqlakeyset.paging import (
    core_page_from_rows,
    prepare_paging,
)
from sqlalchemy import (
    Row,
    Select,
    func,
)

from .orm import Model

P = TypeVar("P", bound=Union[str, int, None])

if TYPE_CHECKING:
    from .repository import SQLAlchemyRepository


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


class PaginationStrategy(Generic[P], ABC):
    def __init__(self, repository: SQLAlchemyRepository | None = None) -> None:
        self._repository = repository

    @property
    def repository(self) -> SQLAlchemyRepository:
        if not self._repository:
            raise ValueError("Repository not set")
        return self._repository

    def __get__(
        self, instance: SQLAlchemyRepository | None, owner: type[SQLAlchemyRepository]
    ) -> PaginationStrategy:
        if instance:
            return self.__class__(instance)
        return self

    @abstractmethod
    async def paginate(
        self,
        page: P,
        page_size: int = 100,
        as_model: bool = True,
        **kwargs,
    ) -> BasePage:
        raise NotImplementedError


class TokenPaginationStrategy(PaginationStrategy[Union[str, None]]):
    def _convert_to_models(self, page_result: Any) -> list[Model]:
        return [p[0] for p in page_result]

    async def paginate(
        self,
        page: str | None = None,
        page_size: int = 100,
        as_model: bool = True,
        **kwargs,
    ) -> BasePage:
        place, backwards = unserialize_bookmark(page)
        sel = prepare_paging(
            q=self.repository.query,
            per_page=page_size,
            place=place,
            backwards=backwards,
            orm=False,
            dialect=self.repository.db.engine.dialect,
        )
        selected = await self.repository.execute_query(sel.select)
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


class NumberedPaginationStrategy(PaginationStrategy[int]):
    async def paginate(
        self,
        page: int = 1,
        page_size: int = 100,
        as_model: bool = True,
        include_total: bool = True,
        **kwargs,
    ) -> BasePage:
        offset = (page - 1) * page_size
        page_query = self.repository.query.offset(offset).limit(page_size)

        if include_total:
            total_records_query = Select(func.count()).select_from(
                self.repository.query.subquery()
            )
            queries = (total_records_query, page_query)
            results = [result async for result in self.repository.execute_many(queries)]
            total_records = results[0].scalar()
            page_result = results[1]
            total_pages = (total_records + page_size - 1) // page_size
        else:
            total_records = None
            total_pages = None
            page_result = await self.repository.execute_query(page_query)

        items = page_result.all() if as_model else page_result.mappings().all()

        return NumberedPage(
            items=items,
            current_page=page,
            page_size=len(items),
            total_pages=total_pages,
            total_items=total_records,
        )
