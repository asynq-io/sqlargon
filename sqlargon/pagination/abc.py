from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    TypedDict,
    TypeVar,
)

from typing_extensions import ParamSpec

from .models import BasePage

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from contextlib import AbstractAsyncContextManager

    from sqlalchemy import Result
    from sqlalchemy.ext.asyncio import AsyncSession

    from sqlargon.query_builder import QueryBuilder

PageT = TypeVar("PageT", bound=BasePage)


class PaginationOptions(TypedDict, total=False):
    as_model: bool
    unique: bool


ParamsT = ParamSpec("ParamsT")


class PaginationStrategy(ABC, Generic[ParamsT, PageT]):
    pagination_options: ClassVar[PaginationOptions] = {"as_model": True, "unique": True}
    query: Any
    session: Callable[[], AbstractAsyncContextManager[AsyncSession]]

    @property
    @abstractmethod
    def qb(self) -> QueryBuilder:
        raise NotImplementedError

    @abstractmethod
    async def paginate(self, *args: ParamsT.args, **kwargs: ParamsT.kwargs) -> PageT:
        raise NotImplementedError

    def _get_pagination_results(self, page_result: Result) -> Sequence[Any]:
        as_model = self.pagination_options.get("as_model", True)
        unique = self.pagination_options.get("unique", True)
        partial_result = page_result.scalars() if as_model else page_result.mappings()
        if unique:
            partial_result = partial_result.unique()
        return partial_result.all()


class BaseOffsetLimitPaginationStrategy(PaginationStrategy[ParamsT, PageT]):
    async def _get_offset_limit_page(self, offset: int, limit: int) -> Sequence[Any]:
        page_query, _ = self.qb.page(
            self.query, offset=offset, limit=limit, include_total=False
        )
        async with self.session() as session:
            page_result = await session.execute(page_query)
        return self._get_pagination_results(page_result)


class BaseTotalPaginationStrategy(PaginationStrategy[ParamsT, PageT]):
    async def _get_items_with_total(
        self,
        offset: int,
        limit: int,
    ) -> tuple[Sequence[Any], int]:
        page_query, total_query = self.qb.page(
            self.query, offset=offset, limit=limit, include_total=True
        )
        async with self.session() as session:
            total_records = (await session.execute(total_query)).scalar()
            if total_records == 0:
                return [], 0
            page_result = await session.execute(page_query)
        items = self._get_pagination_results(page_result)
        return items, total_records
