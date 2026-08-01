# Examples

End-to-end recipes. Each one is self-contained; the models below are reused throughout.

```python
# myapp/models.py
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon import Base
from sqlargon.mixins import CreatedUpdatedMixin, SoftDeleteMixin, UUIDV7ModelMixin
from sqlargon.types import JSON


class User(UUIDV7ModelMixin, CreatedUpdatedMixin, SoftDeleteMixin, Base):
    email: Mapped[str] = mapped_column(sa.Unicode(255), unique=True)
    name: Mapped[str] = mapped_column(sa.Unicode(255))
    preferences: Mapped[dict] = mapped_column(JSON(), default=dict)


class Order(UUIDV7ModelMixin, CreatedUpdatedMixin, Base):
    user_id: Mapped[UUID] = mapped_column(sa.ForeignKey("user.id"))
    total: Mapped[int] = mapped_column(sa.Integer())
    status: Mapped[str] = mapped_column(sa.Unicode(32), default="new")
```

## A FastAPI service

Repositories and units of work take no constructor arguments, so they are dependencies as
they are. Nothing in the endpoints knows which database the work lands on.

```python
# myapp/repositories.py
from collections.abc import Sequence

from sqlargon import SQLAlchemyRepository, SQLAlchemyUnitOfWork
from sqlargon.pagination import TotalPageNumberPagination

from .models import Order, User


class UserRepository(SQLAlchemyRepository[User]):
    default_order_by = User.created_at.desc()
    paginate = TotalPageNumberPagination(default_page_size=25)

    async def active(self) -> Sequence[User]:
        return await self.filter(User.not_deleted).all()

    async def by_email(self, email: str) -> User | None:
        return await self.get(email=email)


class OrderRepository(SQLAlchemyRepository[Order]):
    default_order_by = Order.created_at.desc()


class OrdersUow(SQLAlchemyUnitOfWork):
    users: UserRepository
    orders: OrderRepository
```

```python
# myapp/main.py
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from annotated_types import Interval
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from sqlargon import Database, set_default_database

from .repositories import OrdersUow, UserRepository

db = Database.from_env()


@asynccontextmanager
async def lifespan(app: FastAPI):
    set_default_database(db)
    await db.verify_connection()
    yield
    await db.dispose()


app = FastAPI(lifespan=lifespan)

PageNumber = Annotated[int, Interval(ge=1)]
PageSize = Annotated[int, Interval(ge=1, le=100)]


class UserOut(BaseModel):
    id: UUID
    email: str
    name: str

    model_config = {"from_attributes": True}


class UsersPage(BaseModel):
    items: list[UserOut]
    current_page: int
    total_items: int
    total_pages: int

    model_config = {"from_attributes": True}


class OrderIn(BaseModel):
    email: str
    total: int


@app.get("/users", response_model=UsersPage)
async def list_users(
    users: Annotated[UserRepository, Depends()],
    page: PageNumber = 1,
    page_size: PageSize = 25,
):
    return await users.paginate(page, page_size)


@app.get("/users/{user_id}", response_model=UserOut)
async def get_user(user_id: UUID, users: Annotated[UserRepository, Depends()]):
    user = await users.get(id=user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.post("/orders", response_model=UserOut, status_code=201)
async def create_order(data: OrderIn, uow: Annotated[OrdersUow, Depends()]):
    async with uow:
        user = await uow.users.get_or_create(email=data.email, name=data.email)
        await uow.orders.create(user_id=user.id, total=data.total)
        return user
```

Both statements in `create_order` share the unit of work's transaction: if the order insert
fails, the user is not created either.

## Idempotent ingestion

Upserts keyed on a natural key make a job safe to re-run, and `exclude_set` protects columns
that must not be overwritten:

```python
from sqlargon.typing import OnConflictOptions


class UserRepository(SQLAlchemyRepository[User]):
    @property
    def on_conflict(self) -> OnConflictOptions:
        return {
            "index_elements": {"email"},
            "set_": {"name", "preferences", "updated_at"},
            "exclude_set": {"created_at"},
        }


async def ingest(rows: list[dict]) -> None:
    users = UserRepository()
    for start in range(0, len(rows), 1_000):
        await users.bulk_create_or_update(rows[start : start + 1_000])
```

To insert only what is missing and leave existing rows untouched, use `bulk_create`
(`ON CONFLICT DO NOTHING`) instead, and ask for the rows that were actually written:

```python
inserted = await users.bulk_create(rows, return_results=True)
```

## Draining a work queue

`get_chunk_for_update` selects a locked chunk (`FOR UPDATE SKIP LOCKED` where the dialect
supports it), yields it, and then applies a final update — or deletes the rows — in the same
transaction. Several workers can run this loop concurrently without processing the same row
twice:

```python
class OrderRepository(SQLAlchemyRepository[Order]):
    async def process_pending(self) -> int:
        async with self.get_chunk_for_update(
            {"status": "done"},         # applied to the chunk after the block
            limit=100,
            on_="id",
            order_by=Order.created_at,
            status="new",               # filter
        ) as orders:
            for order in orders:
                await handle(order)
            return len(orders)
```

Pass `None` as the first argument to delete the chunk instead of updating it — the shape of
a classic outbox or dead-letter drain.

For read-only batch work, iterate pages instead and keep the reads off the primary:

```python
from sqlargon.pagination import LimitOffsetPagination


class OrderRepository(SQLAlchemyRepository[Order]):
    default_order_by = Order.id
    paginate = LimitOffsetPagination(default_page_size=1_000)


async def export() -> None:
    orders = OrderRepository().using(read_only=True)
    async for page in orders.filter(status="done").paginate.pages():
        await write_csv(page.items)
```

## Streaming large result sets

`stream()` yields rows without loading the whole result into memory:

```python
async def export_emails() -> None:
    users = UserRepository()
    async for row in users.select(User.email).stream():
        await sink.write(row.email)
```

## Multi-tenant sharding

A cluster maps tenants to databases; the unit of work resolves the shard once and pins it:

```python
from fastapi import Header
from sqlargon import Database, DatabaseCluster, ShardRouter, set_default_database

SHARDS = {"eu": "shard_eu", "us": "shard_us"}

cluster = DatabaseCluster(
    {
        "shard_eu": Database("postgresql+asyncpg://shard-eu/app"),
        "shard_us": Database("postgresql+asyncpg://shard-us/app"),
    },
    router=ShardRouter(SHARDS, default="shard_eu"),
    default="shard_eu",
)
set_default_database(cluster)


def tenant_uow(region: str = Header(alias="X-Region")) -> OrdersUow:
    return OrdersUow().using(shard_key=region)


@app.post("/orders")
async def create_order(data: OrderIn, uow: Annotated[OrdersUow, Depends(tenant_uow)]):
    async with uow:
        return await uow.orders.create(total=data.total)
```

A repository can be pointed at a shard the same way: `repo.using(shard_key=region)`. See
[Database Routing](routing.md) for replicas, `ModelRouter` and the full resolution order.

## Reporting against a replica

```python
import sqlalchemy as sa

from sqlargon import read_only, using


class OrderRepository(SQLAlchemyRepository[Order]):
    @read_only
    async def revenue(self) -> int:
        return await self.select(sa.func.sum(Order.total)).scalar() or 0


# or per call site
with using("replica_0"):
    totals = await OrderRepository().count()
```

## A background job with a lock

Named locks keep a scheduled job single-instance across workers (natively on PostgreSQL and
MySQL — see the [dialects reference](reference/dialects.md)):

```python
@db.with_lock(key="nightly-rollup")
@db.atomic
async def nightly_rollup() -> None:
    orders = OrderRepository()
    await orders.update_many({"status": "archived"}, Order.status == "done")
```

## Testing

Point the default database at SQLite (or a disposable PostgreSQL) per test session and let
each test start from a clean schema:

```python
# tests/conftest.py
import pytest

from sqlargon import Database, set_default_database

from myapp.repositories import UserRepository


@pytest.fixture(scope="session")
def db():
    return Database("sqlite+aiosqlite:///:memory:")


@pytest.fixture(autouse=True)
def default_database(db):
    set_default_database(db)
    yield db
    set_default_database(None)


@pytest.fixture
async def schema(db):
    await db.create_all()
    yield
    await db.drop_all()


@pytest.fixture
def users(schema) -> UserRepository:
    return UserRepository()


async def test_create_user(users: UserRepository):
    user = await users.create(email="a@example.com", name="A")
    assert user is not None
    assert await users.count() == 1
```

`set_default_database(None)` clears the default so no state leaks between tests. To exercise
a repository against a different database without touching the default, bind it explicitly:

```python
repo = UserRepository().using(db=other_database)
```

Enable the [connection tracker](reference/settings.md#connection-tracker) to assert that no
connection is left checked out:

```python
db = Database("sqlite+aiosqlite:///:memory:", enable_tracker=True)
...
from sqlargon.tracker import TRACKER

assert not TRACKER.open_connections
```
