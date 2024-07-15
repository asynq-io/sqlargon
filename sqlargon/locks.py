from __future__ import annotations

from asyncio import Lock
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

Locker = Callable[[AsyncSession, int], AbstractAsyncContextManager[None]]

_DEFAULT_LOCK = Lock()


@asynccontextmanager
async def default_lock(_session: AsyncSession, _key: int) -> AsyncIterator[None]:
    async with _DEFAULT_LOCK:
        yield


@asynccontextmanager
async def postgresql_locker(session: AsyncSession, key: int) -> AsyncIterator[None]:
    try:
        await session.execute(text("SELECT pg_advisory_lock(:key)"), {"key": key})
        yield
        await session.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": key})
    except:  # noqa
        await session.rollback()
        raise
    finally:
        await session.close()
