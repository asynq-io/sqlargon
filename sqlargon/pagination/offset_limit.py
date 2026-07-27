from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, overload

from .abc import ModelT, OffsetPaginator, PaginationStrategy
from .models import OffsetPage, TotalOffsetPage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import RowMapping
    from typing_extensions import Self

    from .abc import SupportsPagination


class LimitOffsetPaginator(OffsetPaginator[ModelT]):
    """Paginates a repository with ``offset``/``limit`` parameters."""

    __slots__ = ()

    @overload
    async def __call__(
        self,
        offset: int = ...,
        limit: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> OffsetPage[ModelT]: ...

    @overload
    async def __call__(
        self,
        offset: int = ...,
        limit: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> OffsetPage[RowMapping]: ...

    async def __call__(
        self,
        offset: int = 0,
        limit: int | None = None,
        *,
        as_model: bool = True,
    ) -> OffsetPage[RowMapping] | OffsetPage[ModelT]:
        """Return a single page of at most ``limit`` items starting at ``offset``."""
        return await self._paginate(offset, limit, as_model=as_model)

    async def _paginate(
        self, offset: int, limit: int | None, *, as_model: bool
    ) -> OffsetPage[RowMapping] | OffsetPage[ModelT]:
        size = self._config.resolve_page_size(limit)
        items, has_more = await self._fetch(offset, size, as_model=as_model)
        return OffsetPage(items=items, offset=offset, limit=size, has_more=has_more)

    @overload
    def pages(
        self,
        offset: int = ...,
        limit: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> AsyncIterator[OffsetPage[ModelT]]: ...

    @overload
    def pages(
        self,
        offset: int = ...,
        limit: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> AsyncIterator[OffsetPage[RowMapping]]: ...

    async def pages(
        self,
        offset: int = 0,
        limit: int | None = None,
        *,
        as_model: bool = True,
    ) -> AsyncIterator[OffsetPage[RowMapping] | OffsetPage[ModelT]]:
        """Iterate over consecutive pages, starting at ``offset``."""
        while True:
            page = await self._paginate(offset, limit, as_model=as_model)
            yield page
            if not page.has_more:
                return
            offset += page.limit


class TotalLimitOffsetPaginator(OffsetPaginator[ModelT]):
    """Paginates with ``offset``/``limit`` and counts the matching rows."""

    __slots__ = ()

    @overload
    async def __call__(
        self,
        offset: int = ...,
        limit: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> TotalOffsetPage[ModelT]: ...

    @overload
    async def __call__(
        self,
        offset: int = ...,
        limit: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> TotalOffsetPage[RowMapping]: ...

    async def __call__(
        self,
        offset: int = 0,
        limit: int | None = None,
        *,
        as_model: bool = True,
    ) -> TotalOffsetPage[RowMapping] | TotalOffsetPage[ModelT]:
        """Return a single page along with the total number of matching rows."""
        return await self._paginate(offset, limit, as_model=as_model)

    async def _paginate(
        self, offset: int, limit: int | None, *, as_model: bool
    ) -> TotalOffsetPage[RowMapping] | TotalOffsetPage[ModelT]:
        size = self._config.resolve_page_size(limit)
        items, total = await self._fetch_with_total(offset, size, as_model=as_model)
        return TotalOffsetPage(
            items=items,
            offset=offset,
            limit=size,
            has_more=offset + len(items) < total,
            total_items=total,
        )

    @overload
    def pages(
        self,
        offset: int = ...,
        limit: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> AsyncIterator[TotalOffsetPage[ModelT]]: ...

    @overload
    def pages(
        self,
        offset: int = ...,
        limit: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> AsyncIterator[TotalOffsetPage[RowMapping]]: ...

    async def pages(
        self,
        offset: int = 0,
        limit: int | None = None,
        *,
        as_model: bool = True,
    ) -> AsyncIterator[TotalOffsetPage[RowMapping] | TotalOffsetPage[ModelT]]:
        """Iterate over consecutive pages, starting at ``offset``."""
        while True:
            page = await self._paginate(offset, limit, as_model=as_model)
            yield page
            if not page.has_more:
                return
            offset += page.limit


@dataclass(frozen=True, kw_only=True, slots=True)
class LimitOffsetPagination(PaginationStrategy):
    """Offset/limit pagination: ``?offset=200&limit=100``.

    Suited for background processing and internal APIs. The returned
    :class:`OffsetPage` carries ``has_more`` detected by over-fetching a
    single row, so no COUNT query is issued.
    """

    @overload
    def __get__(self, obj: None, objtype: type | None = None, /) -> Self: ...

    @overload
    def __get__(
        self, obj: SupportsPagination[ModelT], objtype: type | None = None, /
    ) -> LimitOffsetPaginator[ModelT]: ...

    def __get__(
        self,
        obj: SupportsPagination[ModelT] | None,
        _objtype: type | None = None,
        /,
    ) -> Self | LimitOffsetPaginator[ModelT]:
        return self if obj is None else self.bind(obj)

    def bind(self, source: SupportsPagination[ModelT]) -> LimitOffsetPaginator[ModelT]:
        """Bind this strategy to a repository, returning a callable paginator."""
        return LimitOffsetPaginator(source, self)


@dataclass(frozen=True, kw_only=True, slots=True)
class TotalLimitOffsetPagination(PaginationStrategy):
    """Offset/limit pagination that also reports the total number of rows.

    Runs an additional COUNT query in the same session as the page query and
    returns a :class:`TotalOffsetPage`.
    """

    @overload
    def __get__(self, obj: None, objtype: type | None = None, /) -> Self: ...

    @overload
    def __get__(
        self, obj: SupportsPagination[ModelT], objtype: type | None = None, /
    ) -> TotalLimitOffsetPaginator[ModelT]: ...

    def __get__(
        self,
        obj: SupportsPagination[ModelT] | None,
        _objtype: type | None = None,
        /,
    ) -> Self | TotalLimitOffsetPaginator[ModelT]:
        return self if obj is None else self.bind(obj)

    def bind(
        self, source: SupportsPagination[ModelT]
    ) -> TotalLimitOffsetPaginator[ModelT]:
        """Bind this strategy to a repository, returning a callable paginator."""
        return TotalLimitOffsetPaginator(source, self)
