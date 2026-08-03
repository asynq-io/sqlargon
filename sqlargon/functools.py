from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Concatenate, Protocol, TypeVar

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Any

    from .cluster import AnyDatabase
    from .routing import RoutingContext

P = ParamSpec("P")
R = TypeVar("R")


class _HasDatabase(Protocol):
    @property
    def db(self) -> AnyDatabase: ...

    def routing_context(
        self, statement: Any = None, *, read_only: bool = False
    ) -> RoutingContext: ...


S = TypeVar("S", bound=_HasDatabase)


def atomic(
    fn: Callable[Concatenate[S, P], Awaitable[R]],
) -> Callable[Concatenate[S, P], Awaitable[R]]:
    """Run a repository method within a single transaction.

    All queries issued by the decorated method share one ``session_context``
    on the repository's bound database, committed on success and rolled back
    on error.
    """

    @wraps(fn)
    async def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> R:
        async with self.db.session_context(self.routing_context()):
            return await fn(self, *args, **kwargs)

    return wrapper
