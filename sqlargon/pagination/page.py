from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, overload

from .abc import ModelT, OffsetPaginator, PaginationStrategy
from .models import NumberedPage, TotalNumberedPage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import RowMapping
    from typing_extensions import Self

    from .abc import SupportsPagination


class PageNumberPaginator(OffsetPaginator[ModelT]):
    """Paginates a repository with 1-based ``page``/``page_size`` parameters."""

    __slots__ = ()

    @overload
    async def __call__(
        self,
        page: int = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> NumberedPage[ModelT]: ...

    @overload
    async def __call__(
        self,
        page: int = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> NumberedPage[RowMapping]: ...

    async def __call__(
        self,
        page: int = 1,
        page_size: int | None = None,
        *,
        as_model: bool = True,
    ) -> NumberedPage[ModelT] | NumberedPage[RowMapping]:
        """Return a single page of at most ``page_size`` items."""
        return await self._paginate(page, page_size, as_model=as_model)

    async def _paginate(
        self, page: int, page_size: int | None, *, as_model: bool
    ) -> NumberedPage[ModelT] | NumberedPage[RowMapping]:
        size = self._config.resolve_page_size(page_size)
        items, has_more = await self._fetch((page - 1) * size, size, as_model=as_model)
        return NumberedPage(
            items=items, current_page=page, page_size=size, has_more=has_more
        )

    @overload
    def pages(
        self,
        page: int = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> AsyncIterator[NumberedPage[ModelT]]: ...

    @overload
    def pages(
        self,
        page: int = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> AsyncIterator[NumberedPage[RowMapping]]: ...

    async def pages(
        self,
        page: int = 1,
        page_size: int | None = None,
        *,
        as_model: bool = True,
    ) -> AsyncIterator[NumberedPage[RowMapping] | NumberedPage[ModelT]]:
        """Iterate over consecutive pages, starting at ``page``."""
        while True:
            result = await self._paginate(page, page_size, as_model=as_model)
            yield result
            if not result.has_more:
                return
            page += 1


class TotalPageNumberPaginator(OffsetPaginator[ModelT]):
    """Paginates with ``page``/``page_size`` and counts the matching rows."""

    __slots__ = ()

    @overload
    async def __call__(
        self,
        page: int = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> TotalNumberedPage[ModelT]: ...

    @overload
    async def __call__(
        self,
        page: int = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> TotalNumberedPage[RowMapping]: ...

    async def __call__(
        self,
        page: int = 1,
        page_size: int | None = None,
        *,
        as_model: bool = True,
    ) -> TotalNumberedPage[RowMapping] | TotalNumberedPage[ModelT]:
        """Return a single page along with the total number of rows and pages."""
        return await self._paginate(page, page_size, as_model=as_model)

    async def _paginate(
        self, page: int, page_size: int | None, *, as_model: bool
    ) -> TotalNumberedPage[RowMapping] | TotalNumberedPage[ModelT]:
        size = self._config.resolve_page_size(page_size)
        items, total = await self._fetch_with_total(
            (page - 1) * size, size, as_model=as_model
        )
        total_pages = (total + size - 1) // size
        return TotalNumberedPage(
            items=items,
            current_page=page,
            page_size=size,
            has_more=page < total_pages,
            total_items=total,
            total_pages=total_pages,
        )

    @overload
    def pages(
        self,
        page: int = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> AsyncIterator[TotalNumberedPage[ModelT]]: ...

    @overload
    def pages(
        self,
        page: int = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> AsyncIterator[TotalNumberedPage[RowMapping]]: ...

    async def pages(
        self,
        page: int = 1,
        page_size: int | None = None,
        *,
        as_model: bool = True,
    ) -> AsyncIterator[TotalNumberedPage[RowMapping] | TotalNumberedPage[ModelT]]:
        """Iterate over consecutive pages, starting at ``page``."""
        while True:
            result = await self._paginate(page, page_size, as_model=as_model)
            yield result
            if not result.has_more:
                return
            page += 1


@dataclass(frozen=True, kw_only=True, slots=True)
class PageNumberPagination(PaginationStrategy):
    """Page-number pagination: ``?page=3&page_size=25``.

    Page numbers are 1-based. The returned :class:`NumberedPage` carries
    ``has_more`` detected by over-fetching a single row, so no COUNT query
    is issued.
    """

    @overload
    def __get__(self, obj: None, objtype: type | None = None, /) -> Self: ...

    @overload
    def __get__(
        self, obj: SupportsPagination[ModelT], objtype: type | None = None, /
    ) -> PageNumberPaginator[ModelT]: ...

    def __get__(
        self,
        obj: SupportsPagination[ModelT] | None,
        _objtype: type | None = None,
        /,
    ) -> Self | PageNumberPaginator[ModelT]:
        return self if obj is None else self.bind(obj)

    def bind(self, source: SupportsPagination[ModelT]) -> PageNumberPaginator[ModelT]:
        """Bind this strategy to a repository, returning a callable paginator."""
        return PageNumberPaginator(source, self)


@dataclass(frozen=True, kw_only=True, slots=True)
class TotalPageNumberPagination(PaginationStrategy):
    """Page-number pagination that also reports total rows and pages.

    Runs an additional COUNT query in the same session as the page query and
    returns a :class:`TotalNumberedPage`.
    """

    @overload
    def __get__(self, obj: None, objtype: type | None = None, /) -> Self: ...

    @overload
    def __get__(
        self, obj: SupportsPagination[ModelT], objtype: type | None = None, /
    ) -> TotalPageNumberPaginator[ModelT]: ...

    def __get__(
        self,
        obj: SupportsPagination[ModelT] | None,
        _objtype: type | None = None,
        /,
    ) -> Self | TotalPageNumberPaginator[ModelT]:
        return self if obj is None else self.bind(obj)

    def bind(
        self, source: SupportsPagination[ModelT]
    ) -> TotalPageNumberPaginator[ModelT]:
        """Bind this strategy to a repository, returning a callable paginator."""
        return TotalPageNumberPaginator(source, self)
