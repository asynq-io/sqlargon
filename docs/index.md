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

Docs: [https://asynq-io.github.io/sqlargon/](https://asynq-io.github.io/sqlargon/)

Repository: [https://github.com/asynq-io/sqlargon](https://github.com/asynq-io/sqlargon)

---

## Features

- **Repository pattern** — one object wraps async sessions, core queries and ORM models;
  sessions are context-local and resolved at call time, so nothing gets passed around
- **High-level CRUD** — `create`, `get`, `get_or_create`, `create_or_update`, `all`,
  `list`, `count`, `update_one`, `update_many`, `delete_one`, `delete_many` and `remove`
  out of the box
- **Bulk operations** — `bulk_create`, `bulk_create_or_update` and `bulk_update` with
  per-repository conflict handling
- **Query builder** — fluent, dialect-aware statements for upserts, `RETURNING`, advisory
  locks and streaming, with terminal helpers that cast results to `.scalars()`, `.one()`,
  `.mappings()`, ...
- **Multi-dialect** — PostgreSQL, SQLite, MySQL and MariaDB, with capability-gated SQL
  generation per backend
- **Transactions** — `@atomic` and database-scoped `atomic()` blocks, plus named advisory
  locks
- **Unit of work** — repositories declared as annotations on a unit of work share one
  session and one transaction
- **Database routing** — [clusters](routing.md) with read replicas, shards and vertical
  partitioning; `using()`, `read_only` and per-request `use_context`
- **Pagination** — [page-number, offset/limit and keyset cursor](pagination.md) strategies
- **Outbox** — [transactional outbox](outbox.md) with a background relay and eventiq
  integration
- **Cron** — [database-backed scheduler](cron.md) with namespaces and safe multi-instance
  claiming
- **Column types and mixins** — UUID (v4/v7), timestamp, orjson JSON and pydantic-validated
  columns; mixins for UUID keys, created/updated timestamps and soft delete
- **Soft delete** — tombstone-based deletes via `SoftDeleteRepository`
- **Versioned models** — optimistic concurrency with UUID or PostgreSQL `xmin` versions
- **Auditable models** — [append-only versioned history](auditable.md) with point-in-time
  reads and restore
- **Vector search** — [embeddings with similarity, full-text and hybrid
  reciprocal-rank-fusion search](vectors.md) on PostgreSQL and SQLite
- **FastAPI-ready** — repositories and units of work work directly as dependencies
- **Alembic migrations** — async-first [migration setup](migrations.md)
- **OpenTelemetry** — optional SQLAlchemy instrumentation


## Installation

```shell
pip install sqlargon
```

or

```shell
uv add sqlargon
```

Drivers and optional features ship as extras:

| Extra | Contents |
| --- | --- |
| `postgres` | `asyncpg` |
| `sqlite` | `aiosqlite` |
| `mysql` | `asyncmy` |
| `pagination` | `sqlakeyset`, required for cursor pagination |
| `cron` | `croniter`, `anyio` |
| `outbox` | `anyio` |
| `eventiq` | `eventiq`, required by the outbox integration layer |
| `opentelemetry` | `opentelemetry-instrumentation-sqlalchemy` |
| `standard` | all of the above except `eventiq` |

```shell
pip install "sqlargon[standard]"
```

## Quickstart

```python
import asyncio

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
        return await self.select().filter_by(name=name).one()


async def main() -> None:
    users = UserRepository()
    await users.create(name="John")
    print(await users.list(User.name == "John"))


asyncio.run(main())
```

The model is taken from the generic parameter, and `__init__` takes no arguments — the
repository resolves its database when a statement runs, from `set_default_database(...)`
or from `DATABASE_*` environment variables.

## Where to go next

- **[Usage](usage.md)** — models, CRUD, query building, transactions and units of work.
- **[Database Routing](routing.md)** — replicas, shards, routers and FastAPI wiring.
- **[Pagination](pagination.md)** — page-number, offset/limit and cursor strategies.
- **[Cron](cron.md)** — database-backed scheduling with namespaces and multi-instance safety.
- **[Outbox](outbox.md)** — the transactional outbox pattern and its relay.
- **[Vector Search](vectors.md)** — embeddings, similarity and hybrid search.
- **[Auditable Models](auditable.md)** — append-only versioned history.
- **[Examples](examples.md)** — end-to-end recipes: a FastAPI service, batch workers,
  multi-tenant sharding, testing.
- **Reference** — [types and mixins](reference/types.md), [dialects](reference/dialects.md),
  [settings](reference/settings.md) and the [API reference](reference/api.md).
- **[Alembic Migrations](migrations.md)** — async-only migration setup.
