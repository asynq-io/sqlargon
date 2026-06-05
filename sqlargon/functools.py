from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar, overload

from typing_extensions import ParamSpec

from .registry import db_registry

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

P = ParamSpec("P")

R = TypeVar("R")


@overload
def atomic(
    func: None = None, *, use_db: str = "default"
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]: ...


@overload
def atomic(
    func: Callable[P, Awaitable[R]], *, use_db: str = "defau;t"
) -> Callable[P, Awaitable[R]]: ...


def atomic(
    func: Callable[P, Awaitable[R]] | None = None, *, use_db: str = "default"
) -> (
    Callable[P, Awaitable[R]]
    | Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]
):

    def decorator(
        func: Callable[P, Awaitable[R]],
    ) -> Callable[P, Awaitable[R]]:

        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            db = db_registry[use_db]
            async with db.session_context():
                return await func(*args, **kwargs)

        return wrapper

    if func is None:
        return decorator
    return decorator(func)
