from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Concatenate, Protocol, TypeVar

from typing_extensions import ParamSpec

from .routing import RoutingContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Any

    from .cluster import AnyDatabase

P = ParamSpec("P")
R = TypeVar("R")


class _HasDatabase(Protocol):
    @property
    def db(self) -> AnyDatabase: ...

    def routing_context(
        self, statement: Any = None, *, read_only: bool | None = None
    ) -> RoutingContext: ...


S = TypeVar("S", bound=_HasDatabase)


def atomic(
    fn: Callable[Concatenate[S, P], Awaitable[R]],
) -> Callable[Concatenate[S, P], Awaitable[R]]:
    """Run a repository method within a single transaction.

    All queries issued by the decorated method share one ``session_context``
    on the repository's bound database, committed on success and rolled back
    on error. Objects without a ``routing_context`` method (only the ``db``
    property) are still accepted; their statements route with ambient
    :func:`using` markers alone.
    """

    @wraps(fn)
    async def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> R:
        routing_context = getattr(self, "routing_context", RoutingContext.create)
        async with self.db.session_context(routing_context()):
            return await fn(self, *args, **kwargs)

    return wrapper
