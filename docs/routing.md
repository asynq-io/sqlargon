# Database routing

SQLArgon separates *engines* from *routing policy*:

- `Database` — a single (writable) database: one engine, sessions, query builder.
- `Router` — a pure policy object that picks a database for a statement.
- `DatabaseCluster` — named `Database` objects behind a `Router`, exposing the
  same execution API as a single `Database`.

Repositories and units of work bind to either a `Database` or a
`DatabaseCluster` interchangeably, so the same repository can run against one
database, a primary with read replicas, or a set of shards.

## Default database

No binding is required in the common case: a repository (or unit of work)
without an explicit `database` uses the process-wide default, built lazily
from `DATABASE_*` settings (`DATABASE_URL`, `DATABASE_READ_REPLICAS`,
`DATABASE_AUTO_ROUTE`, `DATABASE_REPLICA_STRATEGY`, ...):

```python
class UserRepository(SQLAlchemyRepository[User]):
    pass  # uses the default database

set_default_database(db)  # or configure it explicitly at startup
```

Repositories and units of work resolve their database to a `using(db=...)`
override if given, otherwise the default; repositories accessed through a
unit of work are bound to the unit's database.

Clusters build from the same settings with `DatabaseCluster.from_env()` —
always a cluster (single-primary when no replicas are configured), with an
optional `router=` override for custom topologies over the configured
databases. The full list of variables is in the
[settings reference](reference/settings.md).

## Read replicas

```python
from sqlargon import DatabaseCluster, SQLAlchemyRepository

db = DatabaseCluster.with_replicas(
    "postgresql+asyncpg://primary/app",
    read_replicas=["postgresql+asyncpg://replica-1/app"],
    auto_route=True,  # SELECTs go to replicas automatically
)

class UserRepository(SQLAlchemyRepository[User]):
    database = db
```

With `auto_route=False`, reads stay on the primary unless explicitly opted in
with `read_only`:

```python
from sqlargon import read_only

class UserRepository(SQLAlchemyRepository[User]):
    database = db

    @read_only
    async def active(self) -> Sequence[User]:
        return await self.filter(is_active=True).all()
```

Replicas are `ReadOnlyDatabase` objects: any DML executed against one raises
`ReadOnlyError` at the engine level, independent of routing.

## Explicit choice: `using`

`using` marks a routing preference for a block, a function or a repository:

```python
from sqlargon import using

with using("replica_0"):                 # context manager (a context variable
    users = await repo.all()             # under the hood, so plain `with`)

@using(read_only=True)                   # decorator
async def report() -> None: ...

await repo.using("replica_0").all()      # per-repository copy
await repo.using(shard_key=tenant_id).create(**values)
```

## FastAPI

Repository and unit-of-work `__init__` take no arguments, so subclasses
work directly as dependencies — no routing knobs leak into the endpoint
signature:

```python
from typing import Annotated

from fastapi import Depends, FastAPI

app = FastAPI()

@app.get("/users")
async def list_users(repo: Annotated[UserRepository, Depends()]) -> list[UserOut]:
    return await repo.all()

@app.post("/orders")
async def create_order(
    data: OrderIn, uow: Annotated[OrdersUow, Depends()]
) -> OrderOut:
    async with uow:
        return await uow.orders.create(**data.model_dump())
```

To route a whole endpoint, attach `use_context` as a dependency — it applies
`using(...)` for the span of the request, so every repository and unit of
work resolved in it follows the preference:

```python
from sqlargon import use_context

@app.get("/users", dependencies=[Depends(use_context("replica_0"))])
async def list_users(repo: UserRepository = Depends()) -> list[UserOut]:
    return await repo.all()
```

For request-scoped routing (e.g. tenant sharding), wrap the repository in a
small dependency:

```python
def user_repository(tenant_id: str = Header()) -> UserRepository:
    return UserRepository().using(shard_key=tenant_id)

@app.get("/users")
async def list_users(repo: UserRepository = Depends(user_repository)) -> list[UserOut]:
    return await repo.all()
```

## Custom topologies

A cluster takes named databases plus a router. Built-in routers:

- `DefaultRouter(name)` — everything to one database.
- `PrimaryReplicaRouter` — writes to the primary, reads to replicas
  (`random` or `round_robin`); with `sticky_reads` (default) reads issued
  after a write in the same async context stay on the primary.
- `ModelRouter` — vertical partitioning: map model classes, table names or a
  model's `__database__` attribute to databases.
- `ShardRouter` — horizontal partitioning: map a shard key (from
  `using(shard_key=...)` or a unit of work) to a database.

```python
from sqlargon import Database, DatabaseCluster, ShardRouter

cluster = DatabaseCluster(
    {
        "shard_0": Database("postgresql+asyncpg://shard-0/app"),
        "shard_1": Database("postgresql+asyncpg://shard-1/app"),
    },
    router=ShardRouter({0: "shard_0", 1: "shard_1"}),
    default="shard_0",
)

set_default_database(cluster)

class OrdersUow(SQLAlchemyUnitOfWork):
    orders: OrderRepository

async with OrdersUow().using(shard_key=tenant_shard(tenant_id)) as uow:
    await uow.orders.create(**values)
```

Any object with a `route(databases, context)` method is a valid router, so
custom policies (e.g. tenant lookup tables) plug in directly.

## Routing rules

For every statement, the target database is resolved in order:

1. **Pinned transaction** — inside an open `session_context` / unit of work,
   every statement uses the pinned database. A conflicting explicit hint
   raises `RoutingError` instead of silently splitting the transaction.
2. **Explicit hint** — `using(...)`, `repo.using(...)` or `uow.using(...)`
   (`hint` / `read_only` / `shard_key`).
3. **Router decision** — based on the statement (read/write), the model and
   the shard key.
4. **Default database.**

Constraints, by design:

- A transaction never spans databases (there is no two-phase commit).
- All members of a cluster must share one SQL dialect, because queries are
  built before they are routed; mixing dialects raises at construction time.
- `create_all` / `drop_all` on a cluster fan out to all writable members.
