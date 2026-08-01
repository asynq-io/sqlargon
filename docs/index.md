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
Version: 1.0.0b1

Docs: [https://asynq-io.github.io/sqlargon/](https://asynq-io.github.io/sqlargon/)

Repository: [https://github.com/asynq-io/sqlargon](https://github.com/asynq-io/sqlargon)

---

## About

SQLArgon provides glue code to use SQLAlchemy async sessions, core queries and ORM models
from one object which provides somewhat of a repository pattern. This solution has a few
advantages:

- no need to pass a `session` object to every function/method — sessions are context-local
  and resolved by the repository itself
- write data access queries in one place
- no need to import `insert`, `update`, `delete`, `select` from SQLAlchemy over and over again
- implicit cast of results to `.scalars().all()`, `.one()`, `.mappings()`, ...
- a dialect-aware query builder for upserts, `RETURNING` and advisory locks
- your view model (e.g. FastAPI routes) does not need to know about the underlying storage —
  the repository class can be replaced at any moment with any object providing a similar
  interface
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

Drivers and optional features ship as extras:

| Extra | Contents |
| --- | --- |
| `postgres` | `asyncpg` |
| `sqlite` | `aiosqlite` |
| `mysql` | `asyncmy` |
| `pagination` | `sqlakeyset`, required for cursor pagination |
| `cron` | `croniter`, `anyio` |
| `opentelemetry` | `opentelemetry-instrumentation-sqlalchemy` |
| `standard` | all of the above |

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
- **[Examples](examples.md)** — end-to-end recipes: a FastAPI service, batch workers,
  multi-tenant sharding, testing.
- **Reference** — [types and mixins](reference/types.md), [dialects](reference/dialects.md),
  [settings](reference/settings.md) and the [API reference](reference/api.md).
- **[Alembic Migrations](migrations.md)** — async-only migration setup.
