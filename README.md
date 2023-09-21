# SQLArgon

![CI](https://github.com/performancemedia/sqlargon/workflows/CI/badge.svg)
![Build](https://github.com/performancemedia/sqlargon/workflows/Publish/badge.svg)
![License](https://img.shields.io/github/license/performancemedia/sqlargon)
![Python](https://img.shields.io/pypi/pyversions/sqlargon)
![Format](https://img.shields.io/pypi/format/sqlargon)
![PyPi](https://img.shields.io/pypi/v/sqlargon)
![Mypy](https://img.shields.io/badge/mypy-checked-blue)
![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)


*Wrapper around SQLAlchemy async session, core and Postgres native features*

---
Version: 0.2.0

Documentation: https://performancemedia.github.io/sqlargon/

Repository: https://github.com/performancemedia/sqlargon

---

## About

This library provides glue code to use sqlalchemy async sessions, core queries and orm models
from one object which provides somewhat of repository pattern. This solution has few advantages:

- no need to pass `session` object to every function/method. It is stored (and optionally injected) in repository object
- write data access queries in one place
- no need to import `insert`,`update`, `delete`, `select` from sqlalchemy over and over again
- Implicit cast of results to `.scalars().all()` or `.one()`
- Your view model (e.g. FastAPI routes) does not need to know about the underlying storage. Repository class can be replaced at any moment any object providing similar interface.

## Usage

```python
import sqlalchemy as sa
from sqlalchemy.orm import Mapped
from sqlargon import GUID, GenerateUUID, Database, Base, SQLAlchemyRepository

db = Database(url=...)

class User(Base):
        id = sa.Column(
            GUID(), primary_key=True, server_default=GenerateUUID(), nullable=False
        )
        name: Mapped[str] = sa.Column(sa.Unicode(255))


class UserRepository(SQLAlchemyRepository[User]):

    async def get_user_by_name(self, name: str):
        # custom user function
        return await self.select().filter_by(name=name).one()

user_repository = UserRepository(...)

# select
await user_repository.all()
await user_repository.select().where(User.name == "test", User.id >= 18)

# insert
user = await user_repository.insert({"name": "test"}).one()

await user_repository.commit()



# delete
await user_repository.delete().filter(name="John").one()

# custom sqlalchemy core functions

users = await user_repository.select().join(...).filter(
    User.name == "test"
).filter_by(...).order_by(User.created_at).limit(2).all()

```

## Sessions

Manager object needs `sqlalchemy.ext.asyncio.AsyncSession`, but it's possible
to provide the session object by yourself, by subclassing Manager class e.g.

```python
from sqlargon import Database
from sqlargon.contrib.fastapi import FastapiRepositoryProvider

db = Database(url="sqlite+aiosqlite:///:memory:")
di = FastapiRepositoryProvider(db)


class UserRepository(Repository[User]):
    ...


from fastapi import FastAPI

app = FastAPI()


@app.get("/users")
async def get_users(user_repository: UserRepository = di[UserRepository]):
    return await user_repository.all()

```
