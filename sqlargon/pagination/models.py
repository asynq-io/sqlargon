from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Sequence

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Page(Generic[T]):
    """Base page shape shared by all pagination strategies."""

    items: Sequence[T]

    @property
    def items_on_page(self) -> int:
        return len(self.items)


@dataclass(frozen=True, slots=True)
class OffsetPage(Page[T]):
    """Page of results addressed by ``offset``/``limit``."""

    offset: int
    limit: int
    has_more: bool


@dataclass(frozen=True, slots=True)
class TotalOffsetPage(OffsetPage[T]):
    """Offset page including the total number of matching rows."""

    total_items: int


@dataclass(frozen=True, slots=True)
class NumberedPage(Page[T]):
    """Page of results addressed by a 1-based page number."""

    current_page: int
    page_size: int
    has_more: bool


@dataclass(frozen=True, slots=True)
class TotalNumberedPage(NumberedPage[T]):
    """Numbered page including the total number of matching rows and pages."""

    total_pages: int
    total_items: int


@dataclass(frozen=True, slots=True)
class CursorPage(Page[T]):
    """Page of results addressed by opaque keyset cursors."""

    cursor: str | None
    next_page: str | None
    previous_page: str | None

    @property
    def has_next(self) -> bool:
        return self.next_page is not None

    @property
    def has_previous(self) -> bool:
        return self.previous_page is not None
