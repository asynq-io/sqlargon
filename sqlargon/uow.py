from __future__ import annotations

from abc import abstractmethod
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from typing import TYPE_CHECKING, Any, ClassVar, get_type_hints

# AnyDatabase is imported at runtime: subclass creation resolves the
# ``database`` annotation via ``get_type_hints`` in this module's namespace.
from .cluster import AnyDatabase
from .registry import get_default_database
from .repository import SQLAlchemyRepository
from .routing import RoutingContext, RoutingError, RoutingOptions

if TYPE_CHECKING:
    from types import TracebackType

    from sqlalchemy.ext.asyncio import AsyncSession
    from typing_extensions import Self

    from .orm import Base


class _RepositoryDescriptor:
    """Provide unit-of-work-scoped repositories bound to the unit's database."""

    def __init__(self, repository_cls: type[SQLAlchemyRepository]) -> None:
        self.repository_cls = repository_cls

    def __get__(
        self, instance: SQLAlchemyUnitOfWork | None, owner: type[SQLAlchemyUnitOfWork]
    ) -> type[SQLAlchemyRepository] | SQLAlchemyRepository:
        if instance is None:
            return self.repository_cls
        return self.repository_cls().using(db=instance.db)


class AbstractUnitOfWork(AbstractAsyncContextManager):
    @abstractmethod
    async def commit(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError


class SQLAlchemyUnitOfWork(AbstractUnitOfWork):
    """Unit of work over a single database or cluster.

    The database is a :meth:`using` override if given, otherwise the
    process-wide default built from ``DATABASE_*`` settings. Declared
    repositories are bound to that database, so all work inside the unit
    shares one transaction -- a unit of work never spans databases (there is
    no two-phase commit).

    ``__init__`` deliberately takes no arguments so subclasses work directly
    as FastAPI dependencies (``Depends(OrdersUow)``) without leaking routing
    knobs as query parameters. Per-use routing (e.g. a shard) is chosen with
    :meth:`using`; on a cluster the member database is resolved once on
    ``__aenter__`` and pinned for the whole transaction::

        async with OrdersUow().using(shard_key=tenant_id) as uow:
            await uow.orders.create(**values)
    """

    database: ClassVar[AnyDatabase | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        hints = get_type_hints(cls)
        for name, hint in hints.items():
            if (
                isinstance(hint, type)
                and issubclass(hint, SQLAlchemyRepository)
                and not isinstance(cls.__dict__.get(name), _RepositoryDescriptor)
            ):
                setattr(cls, name, _RepositoryDescriptor(hint))

    def __init__(self) -> None:
        self._stack = AsyncExitStack()
        self._session: AsyncSession | None = None
        self.routing = RoutingOptions()

    def using(
        self,
        hint: str | None = None,
        *,
        db: AnyDatabase | None = None,
        read_only: bool | None = None,
        shard_key: Any | None = None,
    ) -> Self:
        """Return a fresh unit of work with a routing preference baked in.

        The returned unit is not yet entered, so this is safe to call on a
        dependency-injected instance::

            async with uow.using(shard_key=tenant_id):
                ...
        """
        clone = self.__class__()
        clone.routing = self.routing.merge(
            hint, db=db, read_only=read_only, shard_key=shard_key
        )
        return clone

    @property
    def db(self) -> AnyDatabase:
        if self.routing.db is not None:
            return self.routing.db
        if self.database is not None:
            return self.database
        return get_default_database()

    @property
    def session(self) -> AsyncSession:
        if self._session is None:
            msg = "Session not initialized"
            raise ValueError(msg)
        return self._session

    @classmethod
    def _repository_models(cls) -> list[type[Base]]:
        """Models of the declared repositories, in declaration order."""
        models: list[type[Base]] = []
        for klass in cls.__mro__:
            for attribute in vars(klass).values():
                if isinstance(attribute, _RepositoryDescriptor):
                    model = getattr(attribute.repository_cls, "model", None)
                    if model is not None and model not in models:
                        models.append(model)
        return models

    def _routing_context(self, model: type[Base] | None = None) -> RoutingContext:
        return RoutingContext.create(
            model=model,
            hint=self.routing.hint,
            read_only=self.routing.read_only,
            shard_key=self.routing.shard_key,
        )

    async def __aenter__(self) -> Self:
        if self._session is not None:
            msg = "Cannot open the same unit of work more than once"
            raise ValueError(msg)
        models = self._repository_models()
        contexts = [self._routing_context(model) for model in models] or [
            self._routing_context()
        ]
        if len(contexts) > 1:
            routed = {self.db.route(context) for context in contexts}
            if len(routed) > 1:
                msg = (
                    "Repositories of this unit of work route to different "
                    "databases; a unit of work cannot span databases"
                )
                raise RoutingError(msg)
        await self._stack.__aenter__()
        self._session = await self._stack.enter_async_context(
            self.db.session_context(contexts[0])
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._session = None
        await self._stack.__aexit__(exc_type, exc_val, exc_tb)

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
