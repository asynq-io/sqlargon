from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

if TYPE_CHECKING:
    from collections.abc import Sequence
    from contextlib import AbstractAsyncContextManager

    from sqlalchemy import MappingResult, Result, ScalarResult, Select
    from sqlalchemy.ext.asyncio import AsyncSession

    from sqlargon.query_builder import QueryBuilder

ModelT = TypeVar("ModelT")


class SupportsPagination(Protocol[ModelT]):
    """Structural view of a repository, as required by pagination strategies."""

    model: type[ModelT]

    @property
    def query(self) -> Select[tuple[ModelT]]: ...

    @property
    def qb(self) -> QueryBuilder: ...

    def session(
        self, statement: Any = None, *, read_only: bool | None = None
    ) -> AbstractAsyncContextManager[AsyncSession]: ...


@dataclass(frozen=True, kw_only=True, slots=True)
class PaginationStrategy(ABC):
    """Immutable pagination configuration, attached to a repository class.

    Concrete strategies act as descriptors: accessed on a repository instance
    they return a :class:`Paginator` bound to it, typed with the repository's
    model, so a repository declares its pagination once::

        class UserRepository(SQLAlchemyRepository[User]):
            paginate = PageNumberPagination(default_page_size=25)


        page = await UserRepository().paginate(page=2)  # NumberedPage[User]

    Paging inputs are not validated; constrain them at the API boundary,
    e.g. with pydantic-typed FastAPI parameters.
    """

    default_page_size: int = 100
    unique: bool = False

    @abstractmethod
    def bind(self, source: SupportsPagination[Any]) -> Paginator[Any]:
        """Bind this strategy to a repository, returning a callable paginator."""

    def resolve_page_size(self, page_size: int | None) -> int:
        """Return the requested page size, or the configured default."""
        return self.default_page_size if page_size is None else page_size


class Paginator(Generic[ModelT]):
    """A pagination strategy bound to a repository instance.

    Concrete paginators are awaitable callables returning a page and expose
    a ``pages()`` async iterator walking consecutive pages until exhaustion.
    """

    __slots__ = ("_config", "_source")

    def __init__(
        self, source: SupportsPagination[ModelT], config: PaginationStrategy
    ) -> None:
        self._source = source
        self._config = config

    def _items(self, result: Result[Any], *, as_model: bool) -> Sequence[Any]:
        results: ScalarResult[Any] | MappingResult = (
            result.scalars() if as_model else result.mappings()
        )
        if self._config.unique:
            results = results.unique()
        return results.all()


class OffsetPaginator(Paginator[ModelT]):
    """Shared fetch machinery for offset-based paginators."""

    __slots__ = ()

    async def _fetch(
        self, offset: int, limit: int, *, as_model: bool
    ) -> tuple[Sequence[Any], bool]:
        """Fetch one page plus one extra row to detect whether more rows exist."""
        page_query, _ = self._source.qb.page(
            self._source.query, offset=offset, limit=limit + 1, include_total=False
        )
        async with self._source.session(page_query) as session:
            result = await session.execute(page_query)
        items = self._items(result, as_model=as_model)
        return items[:limit], len(items) > limit

    async def _fetch_with_total(
        self, offset: int, limit: int, *, as_model: bool
    ) -> tuple[Sequence[Any], int]:
        """Fetch one page and the total row count within a single session."""
        page_query, total_query = self._source.qb.page(
            self._source.query, offset=offset, limit=limit, include_total=True
        )
        async with self._source.session(page_query) as session:
            total = (await session.execute(total_query)).scalar() or 0
            if total == 0:
                return [], 0
            result = await session.execute(page_query)
        return self._items(result, as_model=as_model), total
