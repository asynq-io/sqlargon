from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, overload

from .abc import ModelT, PaginationStrategy, Paginator
from .models import CursorPage

try:
    from sqlakeyset import unserialize_bookmark
    from sqlakeyset.paging import core_page_from_rows, prepare_paging
except ImportError as e:
    msg = (
        "Cursor pagination requires the 'sqlakeyset' package; "
        "install 'sqlargon[pagination]'"
    )
    raise ImportError(msg) from e


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import RowMapping
    from typing_extensions import Self

    from .abc import SupportsPagination


class CursorPaginator(Paginator[ModelT]):
    """Paginates a repository with keyset cursors, powered by ``sqlakeyset``.

    Requires the repository query to have a deterministic ``ORDER BY``
    (e.g. via ``default_order_by``).
    """

    __slots__ = ()

    @overload
    async def __call__(
        self,
        cursor: str | None = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[True] = ...,
    ) -> CursorPage[ModelT]: ...

    @overload
    async def __call__(
        self,
        cursor: str | None = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> CursorPage[RowMapping]: ...

    async def __call__(
        self,
        cursor: str | None = None,
        page_size: int | None = None,
        *,
        as_model: bool = True,
    ) -> CursorPage[RowMapping] | CursorPage[ModelT]:
        """Return the page addressed by ``cursor`` (the first page if ``None``)."""
        return await self._paginate(cursor, page_size, as_model=as_model)

    async def _paginate(
        self, cursor: str | None, page_size: int | None, *, as_model: bool
    ) -> CursorPage[RowMapping] | CursorPage[ModelT]:
        size = self._config.resolve_page_size(page_size)
        place, backwards = (None, False)
        if cursor:
            place, backwards = unserialize_bookmark(cursor)
        query = self._source.query
        async with self._source.session(query) as session:
            sel = prepare_paging(
                q=query,
                per_page=size,
                place=place,
                backwards=backwards,
                orm=False,
                dialect=session.bind.dialect,
            )
            selected = await session.execute(sel.select)
        keys = list(selected.keys())
        keys = keys[: len(keys) - len(sel.extra_columns)]
        page_result = core_page_from_rows(
            sel,
            selected.fetchall(),
            keys,
            None,
            size,
            backwards,
            current_place=place,
        )
        items = [row[0] if as_model else row._mapping for row in page_result]  # noqa: SLF001
        paging = page_result.paging
        return CursorPage(
            items=items,
            cursor=cursor,
            next_page=paging.bookmark_next if paging.has_next else None,
            previous_page=paging.bookmark_previous if paging.has_previous else None,
        )

    @overload
    def pages(
        self,
        cursor: str | None = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[True],
    ) -> AsyncIterator[CursorPage[ModelT]]: ...

    @overload
    def pages(
        self,
        cursor: str | None = ...,
        page_size: int | None = ...,
        *,
        as_model: Literal[False],
    ) -> AsyncIterator[CursorPage[RowMapping]]: ...

    async def pages(
        self,
        cursor: str | None = None,
        page_size: int | None = None,
        *,
        as_model: bool = True,
    ) -> AsyncIterator[CursorPage[RowMapping] | CursorPage[ModelT]]:
        """Iterate forward over consecutive pages, starting at ``cursor``."""
        while True:
            page = await self._paginate(cursor, page_size, as_model=as_model)
            yield page
            if page.next_page is None:
                return
            cursor = page.next_page


@dataclass(frozen=True, kw_only=True, slots=True)
class CursorPagination(PaginationStrategy):
    """Keyset (cursor) pagination for large datasets: ``?cursor=<token>``.

    Cursors are opaque bookmarks produced by ``sqlakeyset``; paging is stable
    under concurrent inserts and cheap on large offsets. The ``unique``
    option does not apply to this strategy.
    """

    @overload
    def __get__(self, obj: None, objtype: type | None = None, /) -> Self: ...

    @overload
    def __get__(
        self, obj: SupportsPagination[ModelT], objtype: type | None = None, /
    ) -> CursorPaginator[ModelT]: ...

    def __get__(
        self,
        obj: SupportsPagination[ModelT] | None,
        _objtype: type | None = None,
        /,
    ) -> Self | CursorPaginator[ModelT]:
        return self if obj is None else self.bind(obj)

    def bind(self, source: SupportsPagination[ModelT]) -> CursorPaginator[ModelT]:
        """Bind this strategy to a repository, returning a callable paginator."""
        return CursorPaginator(source, self)
