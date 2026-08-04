from __future__ import annotations

import itertools
import random
from contextvars import ContextVar, Token
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol, TypeVar

from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.selectable import SelectBase
from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
    from types import TracebackType

    from .cluster import DatabaseCluster
    from .database import Database
    from .orm import Base

P = ParamSpec("P")
R = TypeVar("R")

Operation = Literal["read", "write"]


class RoutingError(RuntimeError):
    """Raised when a statement cannot be routed to a database."""


@dataclass(frozen=True, slots=True)
class RoutingContext:
    """Everything a :class:`Router` may consult to pick a database.

    ``operation`` is derived from the statement type (writes are any DML),
    ``read_only`` records an explicit user request for a read replica, and
    ``hint``/``shard_key`` carry explicit user choices made via
    :func:`using`, repository binding or unit-of-work arguments.
    """

    statement: Any | None = None
    model: type[Base] | None = None
    operation: Operation = "write"
    read_only: bool = False
    hint: str | None = None
    shard_key: Any | None = None

    @classmethod
    def create(
        cls,
        statement: Any | None = None,
        model: type[Base] | None = None,
        *,
        read_only: bool | None = None,
        hint: str | None = None,
        shard_key: Any | None = None,
    ) -> RoutingContext:
        """Build a context, merging explicit arguments with :func:`using` markers.

        Explicit arguments win over ambient context variables: ``read_only``
        is tri-state, so an explicit ``False`` overrides an ambient
        ``using(read_only=True)`` scope while ``None`` inherits it. The
        operation defaults to ``write`` for statements that cannot be proven
        read-only, so unknown statements never leak onto a replica.
        """
        options = _routing_options.get()
        forced = bool(options.read_only if read_only is None else read_only)
        if forced or isinstance(statement, SelectBase):
            operation: Operation = "read"
        elif isinstance(statement, UpdateBase):
            operation = "write"
        else:
            operation = "write"
        return cls(
            statement=statement,
            model=model,
            operation=operation,
            read_only=forced,
            hint=hint if hint is not None else options.hint,
            shard_key=shard_key if shard_key is not None else options.shard_key,
        )


@dataclass(frozen=True, slots=True)
class RoutingOptions:
    """Explicit, ambient routing preferences (as opposed to the per-statement
    :class:`RoutingContext`, which is derived fresh for every execution).

    Carried by repository instances and by the context-local :func:`using`
    marker. Immutable so copies can share it safely; derive a changed variant
    with :meth:`merge`.
    """

    db: Database | DatabaseCluster | None = None
    hint: str | None = None
    read_only: bool | None = None
    shard_key: Any | None = None

    def merge(
        self,
        hint: str | None = None,
        *,
        db: Database | DatabaseCluster | None = None,
        read_only: bool | None = None,
        shard_key: Any | None = None,
    ) -> RoutingOptions:
        """Return a copy with the given preferences overriding unset ones.

        ``read_only`` is tri-state: ``None`` keeps the inherited value, while
        an explicit ``False`` overrides it, so a primary read can be forced
        from within a ``read_only=True`` scope.
        """
        return RoutingOptions(
            db=db if db is not None else self.db,
            hint=hint if hint is not None else self.hint,
            read_only=self.read_only if read_only is None else read_only,
            shard_key=shard_key if shard_key is not None else self.shard_key,
        )

    def context(
        self,
        statement: Any | None = None,
        model: type[Base] | None = None,
        *,
        read_only: bool | None = None,
    ) -> RoutingContext:
        """Build the per-statement :class:`RoutingContext` for these options."""
        return RoutingContext.create(
            statement=statement,
            model=model,
            read_only=self.read_only if read_only is None else read_only,
            hint=self.hint,
            shard_key=self.shard_key,
        )


# RoutingOptions is a frozen dataclass, so sharing one default instance is safe.
_NO_OPTIONS: Final[RoutingOptions] = RoutingOptions()
_routing_options: ContextVar[RoutingOptions] = ContextVar(
    "_routing_options", default=_NO_OPTIONS
)


class Router(Protocol):
    """Routing policy: pick a database for a statement.

    Routers are pure policy objects -- they never own engines and are only
    consulted when no transaction is pinned and no explicit hint is set.
    """

    def route(
        self, databases: Mapping[str, Database], context: RoutingContext
    ) -> Database: ...


def _pick(databases: Mapping[str, Database], name: str) -> Database:
    try:
        return databases[name]
    except KeyError:
        msg = f"Unknown database {name!r}; available: {sorted(databases)}"
        raise RoutingError(msg) from None


class UsingContext:
    """Routing marker returned by :func:`using`.

    A synchronous context manager (safe inside async code -- the marker is a
    context variable) and a decorator for async functions. Not reentrant and
    not safe to share between concurrently running tasks; create one per
    scope (the decorator form does this automatically on every call).
    """

    __slots__ = ("_token", "hint", "read_only", "shard_key")

    def __init__(
        self,
        hint: str | None = None,
        *,
        read_only: bool | None = None,
        shard_key: Any | None = None,
    ) -> None:
        self.hint = hint
        self.read_only = read_only
        self.shard_key = shard_key
        self._token: Token[RoutingOptions] | None = None

    def __enter__(self) -> None:
        if self._token is not None:
            msg = "using() marker is not reentrant; create a new one per scope"
            raise RuntimeError(msg)
        merged = _routing_options.get().merge(
            self.hint, read_only=self.read_only, shard_key=self.shard_key
        )
        self._token = _routing_options.set(merged)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._token is not None:
            _routing_options.reset(self._token)
            self._token = None

    def __call__(self, fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with using(self.hint, read_only=self.read_only, shard_key=self.shard_key):
                return await fn(*args, **kwargs)

        return wrapper


def using(
    hint: str | None = None,
    *,
    read_only: bool | None = None,
    shard_key: Any | None = None,
) -> UsingContext:
    """Mark statements in the wrapped scope with a routing preference.

    Use it as a context manager (plain ``with`` -- the marker is a context
    variable, so it composes with async code) or as a decorator for async
    functions::

        with using("replica_eu"):
            users = await repo.all()


        @using(read_only=True)
        async def report() -> None: ...
    """
    return UsingContext(hint, read_only=read_only, shard_key=shard_key)


def use_context(
    hint: str | None = None,
    *,
    read_only: bool | None = None,
    shard_key: Any | None = None,
) -> Callable[[], AsyncIterator[None]]:
    """Build a dependency applying :func:`using` for the span of a request.

    Designed for DI frameworks with generator dependencies (e.g. FastAPI):
    the returned zero-argument async generator sets the routing marker before
    the endpoint runs and resets it afterwards, so every repository or unit
    of work resolved in the same request routes accordingly::

        @app.get("/users", dependencies=[Depends(use_context("replica_0"))])
        async def list_users(repo: UserRepository = Depends()) -> list[UserOut]:
            return await repo.all()
    """

    async def dependency() -> AsyncIterator[None]:
        with using(hint, read_only=read_only, shard_key=shard_key):
            yield

    return dependency


def read_only(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Route reads issued within ``fn`` to a read replica when one is available.

    Use it to opt a repository method into replica routing without enabling
    ``auto_route`` globally::

        class UserRepository(SQLAlchemyRepository[User]):
            @read_only
            async def active(self) -> Sequence[User]:
                return await self.filter(is_active=True).all()
    """
    return using(read_only=True)(fn)


class DefaultRouter:
    """Route every statement to a single named database."""

    def __init__(self, name: str) -> None:
        self.name = name

    def route(
        self,
        databases: Mapping[str, Database],
        context: RoutingContext,  # noqa: ARG002
    ) -> Database:
        return _pick(databases, self.name)


class PrimaryReplicaRouter:
    """Writes go to the primary, reads may go to a replica.

    ``auto_route`` diverts ``SELECT`` statements to replicas; an explicit
    ``read_only`` request always forces a replica. With ``sticky_reads``
    (the default) reads issued after a write in the same async context stay
    on the primary so the caller can read its own writes; disable it for
    long-lived worker tasks where the flag would never reset.
    """

    def __init__(
        self,
        *,
        primary: str = "primary",
        replicas: Sequence[str],
        strategy: Literal["random", "round_robin"] = "random",
        auto_route: bool = True,
        sticky_reads: bool = True,
    ) -> None:
        self.primary = primary
        self.replicas = list(replicas)
        self.auto_route = auto_route
        self.sticky_reads = sticky_reads
        self.strategy = strategy
        self._cycle = itertools.cycle(self.replicas)
        self._wrote: ContextVar[bool] = ContextVar("_wrote", default=False)

    def _pick_replica(self) -> str:
        if self.strategy == "round_robin":
            return next(self._cycle)
        return random.choice(self.replicas)  # noqa: S311  # nosec B311

    def route(
        self, databases: Mapping[str, Database], context: RoutingContext
    ) -> Database:
        if not self.replicas or context.operation == "write":
            if context.operation == "write" and self.sticky_reads:
                self._wrote.set(True)
            return _pick(databases, self.primary)
        if context.read_only:
            return _pick(databases, self._pick_replica())
        if self.sticky_reads and self._wrote.get():
            return _pick(databases, self.primary)
        if self.auto_route and isinstance(context.statement, SelectBase):
            return _pick(databases, self._pick_replica())
        return _pick(databases, self.primary)


class ModelRouter:
    """Route by model: vertical partitioning of tables across databases.

    Resolution order: the explicit ``mapping`` (keyed by model class or table
    name), then the model's ``__database__`` attribute, then ``default``.
    """

    def __init__(
        self,
        mapping: Mapping[type[Any] | str, str] | None = None,
        *,
        default: str,
        attribute: str = "__database__",
    ) -> None:
        self.mapping = dict(mapping or {})
        self.default = default
        self.attribute = attribute

    def _name_for(self, model: type[Any] | None) -> str:
        if model is None:
            return self.default
        if model in self.mapping:
            return self.mapping[model]
        tablename = getattr(model, "__tablename__", None)
        if tablename is not None and tablename in self.mapping:
            return self.mapping[tablename]
        return getattr(model, self.attribute, None) or self.default

    def route(
        self, databases: Mapping[str, Database], context: RoutingContext
    ) -> Database:
        return _pick(databases, self._name_for(context.model))


class ShardRouter:
    """Route by shard key: horizontal partitioning across databases.

    The shard key comes from the routing context (set explicitly or via
    ``using(shard_key=...)``); ``shards`` maps it to a database name, either
    as a mapping or as a callable (e.g. hash-mod). A missing key falls back
    to ``default`` when given, otherwise raises :class:`RoutingError` --
    sharded writes without a key are a caller bug, not a soft default.
    """

    def __init__(
        self,
        shards: Mapping[Any, str] | Callable[[Any], str],
        *,
        default: str | None = None,
    ) -> None:
        self.shards = shards
        self.default = default

    def _name_for(self, shard_key: Any) -> str:
        if callable(self.shards):
            return self.shards(shard_key)
        try:
            return self.shards[shard_key]
        except KeyError:
            msg = f"No shard configured for key {shard_key!r}"
            raise RoutingError(msg) from None

    def route(
        self, databases: Mapping[str, Database], context: RoutingContext
    ) -> Database:
        if context.shard_key is None:
            if self.default is not None:
                return _pick(databases, self.default)
            msg = (
                "No shard key in routing context; set one with "
                "using(shard_key=...) or configure a default shard"
            )
            raise RoutingError(msg)
        return _pick(databases, self._name_for(context.shard_key))
