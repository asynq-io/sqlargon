# Usage

## Databases

A `Database` owns one async engine, its session maker and a dialect-specific
[query builder](reference/dialects.md):

```python
from sqlargon import Database

db = Database(
    url="postgresql+asyncpg://localhost:5432/app",
    echo=True,          # any keyword is forwarded to create_async_engine
    pool_pre_ping=True,
)

db = Database.from_env()  # build from DATABASE_* environment variables
```

Repositories and units of work are not required to know about it: register one default at
startup and everything resolves to it.

```python
from sqlargon import set_default_database

set_default_database(db)
```

Without an explicit default, the first repository call builds one lazily from
[`DATABASE_*` settings](reference/settings.md) — a plain `Database`, or a primary/replica
`DatabaseCluster` when `DATABASE_READ_REPLICAS` is set. See
[Database Routing](routing.md) for clusters, replicas and shards.

Lifecycle helpers, useful in application startup/shutdown hooks and tests:

```python
await db.verify_connection()   # SELECT 1 on every underlying engine
await db.create_all()          # create tables of db.Model.metadata
await db.drop_all()
await db.dispose()             # dispose engines and connection pools
```

## Models

Models subclass `Base` (also exported as `ORMModel`, and available as `db.Model`). Table
names default to the snake-cased class name, and a naming convention for indexes,
constraints and keys is preconfigured so Alembic produces stable names:

```python
import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon import Base


class UserAccount(Base):          # __tablename__ == "user_account"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(sa.Unicode(255))
```

`sqlargon.mixins` provides the usual column sets — `UUIDModelMixin`, `UUIDV7ModelMixin`,
`CreatedUpdatedMixin` and `SoftDeleteMixin` — built on the portable column types described
in the [types reference](reference/types.md):

```python
from sqlargon.mixins import CreatedUpdatedMixin, SoftDeleteMixin, UUIDModelMixin


class User(UUIDModelMixin, CreatedUpdatedMixin, SoftDeleteMixin, Base):
    name: Mapped[str] = mapped_column(sa.Unicode(255))
```

## Repositories

A repository binds one model to a database. The model is read from the generic parameter,
and `__init__` takes no arguments, so subclasses work directly as dependencies:

```python
from sqlargon import SQLAlchemyRepository


class UserRepository(SQLAlchemyRepository[User]):
    default_order_by = User.created_at.desc()

    async def get_by_name(self, name: str) -> User:
        return await self.select().filter_by(name=name).one()


users = UserRepository()
```

`default_order_by` (a column expression or a column name) is applied to the repository's
default query, so `all()`, `list()` and pagination are deterministically ordered.

Three more ways to declare the model, for shared base classes and dynamically built
repositories:

```python
from sqlargon.orm import Model


class R1(SQLAlchemyRepository, model=User): ...            # explicit model


class SoftDeleteRepository(SQLAlchemyRepository[Model], abstract=True):
    """Shared behaviour, no model of its own."""

    async def alive(self):
        return await self.filter(tombstone=False).all()


class R2(SoftDeleteRepository[User]): ...                   # model from the subscript


class AuditedRepository(SQLAlchemyRepository, abstract=True): ...


class R3(AuditedRepository, model=User): ...
```

Useful attributes on every instance: `model`, `db` (the resolved database),
`qb` (the dialect's query builder) and `query` (the statement built so far).

## CRUD

```python
user = await users.create(name="John")                     # None if it conflicts
user = await users.get(name="John")                        # None if missing
user = await users.get(User.id == user_id)
user = await users.get_or_create(name="John")
user = await users.get_or_create({"last_name": "Doe"}, name="John")  # defaults first
user = await users.create_or_update(id=user_id, name="John")         # upsert

all_users = await users.all()
all_users = await users.all(unique=True)
matching = await users.list(User.name == "John")
matching = await users.list(name="John")
total = await users.count(User.name == "John")

user = await users.update_one({"last_name": "Connor"}, User.id == user_id)
await users.update_many({"last_name": "Connor"}, User.name == "John")

user = await users.delete_one(User.id == user_id)
deleted = await users.delete_many(User.name == "John")
await users.remove(User.id == user_id)                     # returns nothing
```

Positional arguments are SQLAlchemy column expressions, keyword arguments are equality
filters (`filter_by`); both can be combined.

Every one of these calls runs in its own transaction, committed when it returns — group
them with [`atomic`](#transactions) when they must succeed or fail together.

## Bulk operations

```python
rows = [{"name": "Alice"}, {"name": "Bob"}]

await users.bulk_create(rows)                              # ON CONFLICT DO NOTHING
created = await users.bulk_create(rows, return_results=True)
await users.bulk_create(rows, ignore_conflicts=False)      # let conflicts raise

await users.bulk_create_or_update(rows)                    # ON CONFLICT DO UPDATE
updated = await users.bulk_create_or_update(rows, return_results=True)

await users.bulk_update(
    values=[{"name": "Alice", "last_name": "Connor"}],
    on_={"name"},                                          # match rows on these columns
)
```

`bulk_update` compiles a single executemany statement with bound parameters, matching each
row on the `on_` columns — one round trip for the whole batch.

### Conflict handling

`insert(..., ignore_conflicts=True)`, `upsert(...)` and the bulk helpers derive their
`ON CONFLICT` clause from the `on_conflict` property, which defaults to the model's primary
key as `index_elements` and every remaining column in `set_`. Override it per repository:

```python
from sqlargon.typing import OnConflictOptions


class UserRepository(SQLAlchemyRepository[User]):
    @property
    def on_conflict(self) -> OnConflictOptions:
        return {
            "index_elements": {"email"},   # or "constraint": "uq_user__email"
            "set_": {"name", "last_name"},
            "exclude_set": {"created_at"},  # never overwritten
            "where": User.tombstone.is_(False),
        }
```

Options can also be passed per call: `await users.upsert(rows, index_elements={"email"})`.
Supported keys are `index_elements`, `constraint`, `index_where`, `set_`, `exclude_set` and
`where`; support per dialect is listed in the [dialects reference](reference/dialects.md).

## Building queries

`select`, `insert`, `upsert`, `update`, `delete`, `where`/`filter`, `join` and `load` return
a repository copy carrying the statement. Any other attribute is proxied to the underlying
SQLAlchemy statement, so `limit`, `offset`, `order_by`, `group_by`, `having`, `distinct`, ...
chain as usual:

```python
recent = await (
    users.select()
    .join(Order, Order.user_id == User.id)
    .filter(User.name == "John")
    .order_by(User.created_at.desc())
    .limit(10)
    .all()
)
```

Awaiting the repository executes the statement and returns a SQLAlchemy `Result`; the
terminal helpers cast it for you:

| Method | Result |
| --- | --- |
| `all(unique=False)` | `Sequence[Model]` |
| `one()` / `one_or_none()` / `first()` | `Model`, or `None` |
| `scalar()` | first column of the first row |
| `scalars()` / `unique()` | `ScalarResult[Model]` |
| `mappings()` | `MappingResult` |
| `stream()` | `AsyncIterator[Row]` |
| `execute()` | `Result` |

```python
user = await users.select().filter(name="John").one_or_none()
name = await users.select(User.name).scalar()
rows = (await users.select(User.id, User.name).mappings()).all()

async for row in users.select().stream():
    ...
```

`load(*keys)` applies `selectinload` for relationships, and `select(with_for_update=...)`
takes `True` or a dict of SQLAlchemy's `with_for_update` options:

```python
await users.select(with_for_update={"skip_locked": True}).limit(100).all()
await users.load(User.orders).all()
```

To run a statement built elsewhere, or several statements in one session:

```python
result = await users.execute_query(sa.select(sa.func.max(User.created_at)))
await users.execute_many(stmt_a, stmt_b)
```

## Transactions

Each repository call commits on its own. Wrap a method in `atomic` to make all of its
queries share one session, committed on success and rolled back on error:

```python
from sqlargon import atomic


class UserRepository(SQLAlchemyRepository[User]):
    @atomic
    async def transfer(self, source_id: UUID, target_id: UUID) -> None:
        await self.update_one({"owner_id": target_id}, User.id == source_id)
        await self.remove(User.id == source_id)
```

The database object offers the same for any coroutine, plus explicit session access:

```python
@db.atomic
async def import_users(rows: list[dict]) -> None: ...


async with db.session_context() as session:      # reuses an open session if there is one
    ...

async with db.session() as session:              # always a fresh session
    await session.execute(sa.text("VACUUM"))
```

`session_context()` is what repositories use internally: nesting it reuses the context-local
session instead of opening a second one, which is why `atomic` composes with repository
methods that are themselves atomic.

## Named locks

`lock` takes a database-native advisory lock where the dialect supports one (Postgres and
MySQL) and falls back to a process-local `asyncio.Lock` otherwise:

```python
async with db.lock("nightly-import"):
    ...


@db.with_lock(key="nightly-import")
async def run_import() -> None: ...
```

## Unit of work

Repositories declared as annotations on a unit of work are bound to the unit's database and
share one session — and therefore one transaction:

```python
from sqlargon import SQLAlchemyUnitOfWork


class OrdersUow(SQLAlchemyUnitOfWork):
    users: UserRepository
    orders: OrderRepository


async with OrdersUow() as uow:
    user = await uow.users.create(name="John")
    await uow.orders.create(user_id=user.id, total=100)
    await uow.commit()
```

`commit()` and `rollback()` act on that shared session; leaving the block without an
exception commits it anyway, and an exception rolls it back — the explicit `commit()` above
is only needed when work follows inside the same block. Opened inside an already-open
session context (another unit of work, or an `atomic` method), the unit joins that session
and the outer scope decides whether the work is committed.

A unit of work never spans databases — there is no two-phase commit. On a cluster the
member database is resolved once on `__aenter__` and pinned for the whole transaction:

```python
async with OrdersUow().using(shard_key=tenant_id) as uow:
    await uow.orders.create(**values)
```

Like repositories, `__init__` takes no arguments (so a unit of work works as a dependency),
`using(...)` returns a fresh, not-yet-entered copy, and the same unit cannot be entered
twice. Subclass `AbstractUnitOfWork` if you need a non-SQLAlchemy implementation behind the
same interface.
