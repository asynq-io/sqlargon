from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class BasePage(Generic[T]):
    items: Sequence[T]


@dataclass
class TotalBasePage(BasePage[T]):
    total_items: int


@dataclass
class CursorPage(BasePage[T]):
    current_page: str | None
    next_page: str | None
    previous_page: str | None


@dataclass
class NumberedPage(BasePage[T]):
    current_page: int
    page_size: int
    items_on_page: int


@dataclass
class TotalNumberedPage(NumberedPage[T]):
    total_pages: int
    total_items: int
