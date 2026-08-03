from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, TypeAlias, TypeVar

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from sqlargon.cluster import AnyDatabase
from sqlargon.registry import get_default_database, set_default_database
from sqlargon.routing import using

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from starlette.types import ASGIApp, Message, Receive, Scope, Send

T = TypeVar("T")

Provide: TypeAlias = Annotated[T, Depends()]
"""Inject a repository or unit of work, resolved from the annotated type."""


class DatabaseMiddleware:
    """Set up the database before the application's own lifespan and dispose it after."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        database: AnyDatabase | None = None,
        verify: bool = True,
        dispose: bool = True,
    ) -> None:
        self.app = app
        self.database = database
        self.verify = verify
        self.dispose = dispose

    @property
    def db(self) -> AnyDatabase:
        if self.database is not None:
            return self.database
        return get_default_database()

    async def startup(self) -> None:
        if self.database is not None:
            set_default_database(self.database)
        if self.verify:
            await self.db.verify_connection()

    async def shutdown(self) -> None:
        if self.dispose:
            await self.db.dispose()
        if self.database is not None:
            set_default_database(None)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "lifespan":
            await self.app(scope, receive, send)
            return

        async def receive_hook() -> Message:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await self.startup()
            return message

        async def send_hook(message: Message) -> None:
            if message["type"].startswith("lifespan.shutdown."):
                await self.shutdown()
            await send(message)

        await self.app(scope, receive_hook, send_hook)


def transaction(
    hint: str | None = None,
    *,
    read_only: bool = False,
    shard_key: Any | None = None,
    database: AnyDatabase | None = None,
) -> Callable[[], AsyncIterator[AsyncSession]]:
    """Build a dependency holding one routed transaction for the span of a request."""

    async def dependency() -> AsyncIterator[AsyncSession]:
        db = get_default_database() if database is None else database
        with using(hint, read_only=read_only, shard_key=shard_key):
            async with db.session_context() as session:
                yield session

    return dependency


Db: TypeAlias = Annotated[AnyDatabase, Depends(get_default_database)]
"""Inject the default database (a single database or a cluster)."""

Session: TypeAlias = Annotated[AsyncSession, Depends(transaction())]
"""Inject a request-scoped session on the default database."""

__all__ = ["DatabaseMiddleware", "Db", "Provide", "Session", "transaction"]
