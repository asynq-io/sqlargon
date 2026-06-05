from .abc import PaginationStrategy
from .cursor import CursorPaginationStrategy
from .offset_limit import (
    OffsetLimitPaginationStrategy,
    TotalOffsetLimitPaginationStrategy,
)
from .page import (
    NumberedPaginationStrategy,
    TotalNumberedPaginationStrategy,
)

TokenPaginationStrategy = CursorPaginationStrategy

__all__ = [
    "CursorPaginationStrategy",
    "NumberedPaginationStrategy",
    "OffsetLimitPaginationStrategy",
    "PaginationStrategy",
    "TokenPaginationStrategy",
    "TotalNumberedPaginationStrategy",
    "TotalOffsetLimitPaginationStrategy",
]
