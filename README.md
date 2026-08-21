# SQLArgon

![Test](https://github.com/asynq-io/sqlargon/workflows/Test/badge.svg)
![Build](https://github.com/asynq-io/sqlargon/workflows/Publish/badge.svg)
![License](https://img.shields.io/github/license/asynq-io/sqlargon)
![Python](https://img.shields.io/pypi/pyversions/sqlargon)
![Format](https://img.shields.io/pypi/format/sqlargon)
![PyPi](https://img.shields.io/pypi/v/sqlargon)
![Mypy](https://img.shields.io/badge/mypy-checked-blue)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/charliermarsh/ruff)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)


*SQLAlchemy repository pattern and utilities*

---

Documentation: https://asynq-io.github.io/sqlargon/

Repository: https://github.com/asynq-io/sqlargon

---

## About

This library provides glue code to use sqlalchemy async sessions, core queries and orm models
from one object which provides somewhat of repository pattern. This solution has few advantages:

- no need to pass `session` object to every function/method. Sessions are context-local and
  resolved by the repository itself
- write data access queries in one place
- no need to import `insert`, `update`, `delete`, `select` from sqlalchemy over and over again
- implicit cast of results to `.scalars().all()`, `.one()`, `.mappings()`, ...
- dialect-aware query builder (Postgres, SQLite, MySQL) for upserts, `RETURNING` and advisory locks
- your view model (e.g. FastAPI routes) does not need to know about the underlying storage.
  Repository class can be replaced at any moment with any object providing similar interface
- engines and routing policy are separate, so the same repository runs against one database,
  a primary with read replicas, or a set of shards

## Installation

```shell
pip install sqlargon
```

or

```shell
uv add sqlargon
```

Optional extras: `postgres`, `sqlite`, `mysql`, `pagination` (cursor pagination),
`cron`, `outbox`, `eventiq`, `opentelemetry`, or `standard` for all of them
except `eventiq`:

```shell
pip install "sqlargon[standard]"
```

## Usage

```python
from typing import Sequence

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon import Base, Database, SQLAlchemyRepository, set_default_database
from sqlargon.mixins import CreatedUpdatedMixin, UUIDModelMixin

set_default_database(Database(url="postgresql+asyncpg://localhost:5432/app"))


class User(UUIDModelMixin, CreatedUpdatedMixin, Base):
    name: Mapped[str] = mapped_column(sa.Unicode(255))
    last_name: Mapped[str | None] = mapped_column(sa.Unicode(255), nullable=True)


class UserRepository(SQLAlchemyRepository[User]):
    default_order_by = User.created_at.desc()

    async def get_by_name(self, name: str) -> User:
        # custom query, built with the repository's query builder
        return await self.select().filter_by(name=name).one()


user_repository = UserRepository()
```

The model is taken from the generic parameter, and `__init__` takes no arguments — the
repository resolves its database at call time (see [Routing](#routing)).

### High level CRUD

```python
user = await user_repository.create(name="John")
user = await user_repository.get(name="John")                # None if missing
user = await user_repository.get_or_create(name="John")
user = await user_repository.create_or_update(id=user_id, name="John")  # upsert

users = await user_repository.all()
users = await user_repository.list(User.name == "John")
count = await user_repository.count(User.name == "John")

user = await user_repository.update_one({"last_name": "Connor"}, User.id == user_id)
await user_repository.update_many({"last_name": "Connor"}, User.name == "John")

user = await user_repository.delete_one(User.id == user_id)
users = await user_repository.delete_many(User.name == "John")
await user_repository.remove(User.id == user_id)             # no results returned
```

### Bulk operations

```python
users = [{"name": "Alice"}, {"name": "Bob"}]

await user_repository.bulk_create(users)                       # ON CONFLICT DO NOTHING
created = await user_repository.bulk_create(users, return_results=True)
await user_repository.bulk_create_or_update(users)             # ON CONFLICT DO UPDATE
await user_repository.bulk_update(
    values=[{"name": "Alice", "last_name": "Connor"}],
    on_={"name"},
)
```

Conflict handling defaults to the model's primary key as `index_elements` and every other
column in `set_`. Override it per repository:

```python
from sqlargon.typing import OnConflictOptions


class UserRepository(SQLAlchemyRepository[User]):
    @property
    def on_conflict(self) -> OnConflictOptions:
        return {"index_elements": {"id"}, "set_": {"name"}, "exclude_set": {"last_name"}}
```

### Building queries

Query methods (`select`, `insert`, `upsert`, `update`, `delete`, `filter`/`where`, `join`,
`load`) return a repository copy carrying the statement; any other attribute is proxied to
the underlying SQLAlchemy statement, so `limit`, `order_by`, `group_by`, ... chain as usual.
Awaiting the repository executes the statement and returns a `Result`; the terminal helpers
cast it for you:

```python
users = await (
    user_repository.select()
    .join(Order, Order.user_id == User.id)
    .filter(User.name == "John")
    .order_by(User.created_at)
    .limit(2)
    .all()
)

user = await user_repository.select().filter(name="John").one_or_none()
name = await user_repository.select(User.name).scalar()
rows = await user_repository.select(User.id, User.name).mappings()
result = await user_repository.insert({"name": "John"}, return_results=True)

async for row in user_repository.select().stream():
    ...
```

Terminal methods: `all(unique=False)`, `one()`, `one_or_none()`, `first()`, `scalar()`,
`scalars()`, `unique()`, `mappings()`, `stream()`, `execute()`.

### Transactions

`atomic` wraps a repository method in a single session, committed on success and rolled back
on error:

```python
from sqlargon import atomic


class UserRepository(SQLAlchemyRepository[User]):
    @atomic
    async def create_users(self, names: Sequence[str]) -> None:
        for name in names:
            await self.create(name=name)
```

The same works on any coroutine via the database object, which also exposes named locks
(database-native advisory locks where the dialect supports them):

```python
db = Database.from_env()


@db.atomic
async def do_work() -> None: ...


@db.with_lock(key="import")
async def import_data() -> None: ...


async with db.lock("import"):
    ...
```

## Unit of work

Repositories declared as annotations on a unit of work share one session and one transaction:

```python
from sqlargon import SQLAlchemyUnitOfWork


class OrdersUow(SQLAlchemyUnitOfWork):
    users: UserRepository
    orders: OrderRepository


async with OrdersUow() as uow:
    user = await uow.users.create(name="John")
    await uow.orders.create(user_id=user.id)
    await uow.commit()
```

A unit of work never spans databases; the member database is resolved once on `__aenter__`
and pinned for the whole transaction.

## Routing

A repository (or unit of work) without an explicit `database` uses the process-wide default,
set with `set_default_database(...)` or built lazily from `DATABASE_*` environment variables
(`DATABASE_URL`, `DATABASE_ECHO`, `DATABASE_POOL_SIZE`, `DATABASE_READ_REPLICAS`, ...):

```python
from sqlargon import Database, DatabaseCluster

db = Database.from_env()                 # single database
cluster = DatabaseCluster.from_env()     # primary + DATABASE_READ_REPLICAS
```

Bind explicitly with the `database` class attribute or per call with `using`:

```python
from sqlargon import DatabaseCluster, read_only, using

db = DatabaseCluster.with_replicas(
    "postgresql+asyncpg://primary/app",
    read_replicas=["postgresql+asyncpg://replica-1/app"],
    auto_route=True,  # SELECTs go to replicas automatically
)


class UserRepository(SQLAlchemyRepository[User]):
    database = db

    @read_only
    async def active(self) -> Sequence[User]:
        return await self.filter(is_active=True).all()


await user_repository.using("replica_0").all()
await user_repository.using(shard_key=tenant_id).create(name="John")

with using(read_only=True):
    users = await user_repository.all()
```

Clusters take named databases plus a router — `DefaultRouter`, `PrimaryReplicaRouter`,
`ModelRouter` (vertical partitioning), `ShardRouter` (horizontal partitioning), or any object
with a `route(databases, context)` method. See the
[routing docs](https://asynq-io.github.io/sqlargon/routing/) for the full resolution order.

## Pagination

Pagination is a strategy attached to a repository class; accessed on an instance it returns a
paginator typed with the repository's model:

```python
from sqlargon.pagination import PageNumberPagination


class UserRepository(SQLAlchemyRepository[User]):
    paginate = PageNumberPagination(default_page_size=25)


page = await UserRepository().filter(User.name == "John").paginate(page=2)
page.items, page.current_page, page.page_size, page.has_more

async for page in UserRepository().paginate.pages(page_size=100):
    ...
```

Available strategies: `PageNumberPagination`, `TotalPageNumberPagination`,
`LimitOffsetPagination`, `TotalLimitOffsetPagination` and `CursorPagination`
(keyset, requires `sqlargon[pagination]`).

## Outbox

`sqlargon.outbox` implements the transactional outbox pattern: a write through the repository
also appends a CloudEvent-shaped row to `outbox_events` **in the same transaction**, so an
event can neither be lost by a rollback nor published for a row that never committed. A
background relay then publishes them in write order (requires `sqlargon[outbox]`):

```python
from sqlargon import Base
from sqlargon.outbox import OutboxConfig, OutboxRelay, OutboxRepository


class User(UUIDModelMixin, CreatedUpdatedMixin, Base):  # is_new tells insert from update
    name: Mapped[str] = mapped_column(sa.Unicode(255))
    password: Mapped[str] = mapped_column(sa.Unicode(255))


class UserRepository(OutboxRepository[User]):
    outbox = OutboxConfig(topic="users", exclude={"password"})


await UserRepository().create(name="John", password=hashed)
# -> one outbox_events row, type "user.created", password left out

async with OutboxRelay(publish).running():  # publish is any async callable
    ...
```

The relay takes a plain publisher callable, so sqlargon depends on no broker client;
`sqlargon.integrations.eventiq` adapts the rows to `eventiq.CloudEvent`. Only writes that go
through the repository are recorded. See the
[outbox docs](https://asynq-io.github.io/sqlargon/outbox/) for retention, retries and
ordering.

## Column types and mixins

`sqlargon.types` provides dialect-aware column types: `GUID` with `GenerateUUID` /
`GenerateUUIDV7` server defaults, `Timestamp` with a `now()` server default and `JSON`
(orjson-serialized). `sqlargon.types.pydantic` adds `Pydantic` and `ValidatedType` for
pydantic-validated columns. `sqlargon.mixins` bundles them into `UUIDModelMixin`,
`UUIDV7ModelMixin`, `CreatedUpdatedMixin` and `SoftDeleteMixin`.

## FastAPI

Repository and unit-of-work `__init__` take no arguments, so subclasses work directly as
dependencies — no routing knobs leak into the endpoint signature:

```python
from fastapi import Depends, FastAPI

app = FastAPI()


@app.get("/users")
async def list_users(repo: UserRepository = Depends()) -> list[UserOut]:
    return await repo.all()


@app.post("/orders")
async def create_order(data: OrderIn, uow: OrdersUow = Depends()) -> OrderOut:
    async with uow:
        return await uow.orders.create(**data.model_dump())
```

To route a whole endpoint, attach `use_context` as a dependency — it applies `using(...)` for
the span of the request:

```python
from sqlargon import use_context


@app.get("/reports", dependencies=[Depends(use_context(read_only=True))])
async def reports(repo: UserRepository = Depends()) -> list[UserOut]:
    return await repo.all()
```

## Tests

The unit suite runs against an in-memory SQLite database and needs nothing installed:

```bash
pytest
```

The end-to-end suite runs the same stack against PostgreSQL, MySQL, MariaDB and an on-disk
SQLite database, spinning the servers up and tearing them down with
[testcontainers](https://testcontainers-python.readthedocs.io) — so it only needs a Docker
daemon. It is skipped unless asked for:

```bash
pytest --e2e                              # unit suite + every backend
pytest --e2e ./tests/e2e                  # e2e only
pytest --e2e --e2e-backends=mysql,mariadb # selected backends
```

See [tests/e2e/README.md](tests/e2e/README.md) for the layout and the per-backend
capability gaps it pins down.
