from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from .database import BaseDatabase, Database, ReadOnlyDatabase
from .routing import DefaultRouter, PrimaryReplicaRouter, RoutingContext, RoutingError
from .routing import using as _using

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Mapping, Sequence
    from typing import Literal

    from sqlalchemy.ext.asyncio import AsyncSession

    from .routing import Router, UsingContext

    ReplicaConfig = str | Mapping[str, Any]


class DatabaseCluster(BaseDatabase):
    """Named databases behind a routing policy, usable wherever a
    :class:`~sqlargon.Database` is.

    Repositories and units of work bind to the cluster; each statement is
    routed to a member database by, in order of precedence:

    1. the database pinned by an open transaction in the current context,
    2. an explicit hint (``using(...)``, repository binding, unit of work),
    3. the :class:`~sqlargon.routing.Router`,

    A hint conflicting with the pinned database raises
    :class:`~sqlargon.RoutingError` instead of silently splitting the
    transaction. All members must share one SQL dialect, since queries are
    built before they are routed.
    """

    def __init__(
        self,
        databases: Mapping[str, Database],
        *,
        router: Router | None = None,
        default: str = "primary",
    ) -> None:
        if not databases:
            msg = "A database cluster requires at least one database"
            raise ValueError(msg)
        if default not in databases:
            msg = f"Default database {default!r} not in {sorted(databases)}"
            raise ValueError(msg)
        dialects = {database.dialect for database in databases.values()}
        if len(dialects) > 1:
            msg = (
                "All databases in a cluster must share one dialect "
                f"(queries are built before routing); got {sorted(dialects)}"
            )
            raise ValueError(msg)
        self.databases = dict(databases)
        self.default = default
        self.dialect = next(iter(dialects))
        self.query_builder = self.databases[default].query_builder
        self.router: Router = router if router is not None else DefaultRouter(default)
        self._pinned: ContextVar[Database | None] = ContextVar("_pinned", default=None)

    @classmethod
    def with_replicas(
        cls,
        url: str,
        *,
        read_replicas: Sequence[ReplicaConfig] | None = None,
        auto_route: bool = False,
        replica_strategy: Literal["random", "round_robin"] = "random",
        **kwargs: Any,
    ) -> DatabaseCluster:
        """Build a primary/replica cluster from connection URLs.

        Each replica is either a URL (inheriting the primary's engine
        options) or a mapping with a ``url`` key plus per-replica engine
        option overrides.
        """
        databases: dict[str, Database] = {"primary": Database(url, **kwargs)}
        for index, spec in enumerate(read_replicas or []):
            if isinstance(spec, str):
                replica_url, overrides = spec, {}
            else:
                overrides = dict(spec)
                replica_url = overrides.pop("url")
            databases[f"replica_{index}"] = ReadOnlyDatabase(
                replica_url, **{**kwargs, **overrides}
            )
        router = PrimaryReplicaRouter(
            replicas=[name for name in databases if name != "primary"],
            auto_route=auto_route,
            strategy=replica_strategy,
        )
        return cls(databases, router=router, default="primary")

    @classmethod
    def from_env(
        cls, *, router: Router | None = None, **kwargs: Any
    ) -> DatabaseCluster:
        """Build a cluster from ``DATABASE_*`` settings.

        Always returns a cluster: primary plus any ``DATABASE_READ_REPLICAS``
        (a single-primary cluster when none are configured, ready to grow).
        Pass ``router`` to replace the primary/replica policy, e.g. with a
        :class:`~sqlargon.ShardRouter` over the configured databases.
        """
        from .settings import DatabaseClusterSettings

        settings = DatabaseClusterSettings(**kwargs)
        cluster = cls.with_replicas(**settings.to_kwargs() | kwargs)
        if router is not None:
            cluster.router = router
        return cluster

    @property
    def default_database(self) -> Database:
        return self.databases[self.default]

    @property
    def replicas(self) -> list[Database]:
        return [
            database
            for database in self.databases.values()
            if isinstance(database, ReadOnlyDatabase)
        ]

    def _database_for(self, name: str) -> Database:
        try:
            return self.databases[name]
        except KeyError:
            msg = f"Unknown database {name!r}; available: {sorted(self.databases)}"
            raise RoutingError(msg) from None

    def route(self, context: RoutingContext | None = None) -> Database:
        """Return the member database the given statement should run against."""
        if context is None:
            context = RoutingContext.create()
        pinned = self._pinned.get()
        if pinned is not None:
            if (
                context.hint is not None
                and self._database_for(context.hint) is not pinned
            ):
                msg = (
                    f"Hint {context.hint!r} conflicts with the database pinned by "
                    "the open transaction; a transaction cannot span databases"
                )
                raise RoutingError(msg)
            return pinned
        if context.hint is not None:
            return self._database_for(context.hint)
        return self.router.route(self.databases, context)

    @asynccontextmanager
    async def session_context(
        self, context: RoutingContext | None = None
    ) -> AsyncGenerator[AsyncSession]:
        """Yield a session on the routed database, pinning it for the context.

        While the session is open, every statement in the same async context
        routes to the same database, so a unit of work never straddles two
        databases.
        """
        database = self.route(context)
        token = None
        if self._pinned.get() is None:
            token = self._pinned.set(database)
        try:
            async with database.session_context() as session:
                yield session
        finally:
            if token is not None:
                self._pinned.reset(token)

    @asynccontextmanager
    async def session(
        self, context: RoutingContext | None = None
    ) -> AsyncGenerator[AsyncSession]:
        async with self.route(context).session() as session:
            yield session

    def using(
        self,
        hint: str | None = None,
        *,
        read_only: bool = False,
        shard_key: Any | None = None,
    ) -> UsingContext:
        """Return a :func:`~sqlargon.using` marker validated against this cluster."""
        if hint is not None and hint not in self.databases:
            msg = f"Unknown database {hint!r}; available: {sorted(self.databases)}"
            raise RoutingError(msg)
        return _using(hint, read_only=read_only, shard_key=shard_key)

    async def verify_connection(self) -> None:
        for database in self.databases.values():
            await database.verify_connection()

    async def dispose(self) -> None:
        for database in self.databases.values():
            await database.dispose()

    async def create_all(self) -> None:
        for database in self._writable_databases():
            await database.create_all()

    async def drop_all(self) -> None:
        for database in self._writable_databases():
            await database.drop_all()

    def _writable_databases(self) -> list[Database]:
        seen: list[Database] = []
        for database in self.databases.values():
            if isinstance(database, ReadOnlyDatabase):
                continue
            if database not in seen:
                seen.append(database)
        return seen


AnyDatabase = Database | DatabaseCluster
