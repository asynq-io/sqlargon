# Settings

`Database.from_env()`, `DatabaseCluster.from_env()` and the lazily built default database all
read their configuration from environment variables (or a `.env` file) through
pydantic-settings, with the `DATABASE_` prefix.

```shell
export DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/app"
export DATABASE_POOL_SIZE=20
export DATABASE_POOL_PRE_PING=true
```

```python
from sqlargon import Database

db = Database.from_env()
db = Database.from_env(echo=True)   # explicit keywords win over the environment
```

## `DatabaseSettings`

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql+asyncpg://localhost:5432` | async SQLAlchemy connection URL |
| `DATABASE_ECHO` | `false` | log every statement |
| `DATABASE_ISOLATION_LEVEL` | — | engine isolation level |
| `DATABASE_JSON_SERIALIZER` | `sqlargon.utils:json_dumps` | callable used to serialize JSON |
| `DATABASE_JSON_DESERIALIZER` | `sqlargon.utils:json_loads` | callable used to deserialize JSON |
| `DATABASE_CONNECT_ARGS` | — | JSON object passed to the DBAPI |
| `DATABASE_ENABLE_TRACKER` | `false` | attach the [connection tracker](#connection-tracker) |
| `DATABASE_POOLCLASS` | — | import path of a `Pool` subclass |
| `DATABASE_POOL_SIZE` | — | connections kept in the pool |
| `DATABASE_MAX_OVERFLOW` | — | connections allowed beyond `pool_size` |
| `DATABASE_ECHO_POOL` | — | log pool checkouts and returns |
| `DATABASE_POOL_RECYCLE` | — | recycle connections after N seconds |
| `DATABASE_POOL_PRE_PING` | — | test connections before handing them out |
| `DATABASE_POOL_TIMEOUT` | — | seconds to wait for a free connection |
| `DATABASE_POOL_USE_LIFO` | — | reuse the most recently returned connection |

Unset options are omitted rather than passed as `None`, so SQLAlchemy's own defaults apply.
Extra `DATABASE_*` variables are allowed and forwarded to `create_async_engine`, which makes
engine options the library does not name explicitly reachable without a code change.

`DATABASE_JSON_SERIALIZER`, `DATABASE_JSON_DESERIALIZER` and `DATABASE_POOLCLASS` are import
strings in `module:object` form:

```shell
export DATABASE_POOLCLASS="sqlalchemy.pool:NullPool"
export DATABASE_JSON_SERIALIZER="myapp.json:dumps"
```

## `DatabaseClusterSettings`

`DatabaseCluster.from_env()` reads everything above plus:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_READ_REPLICAS` | — | JSON array of replica URLs |
| `DATABASE_AUTO_ROUTE` | `true` | send `SELECT`s to replicas automatically |
| `DATABASE_REPLICA_STRATEGY` | `random` | `random` or `round_robin` |

```shell
export DATABASE_URL="postgresql+asyncpg://primary/app"
export DATABASE_READ_REPLICAS='["postgresql+asyncpg://replica-1/app", "postgresql+asyncpg://replica-2/app"]'
export DATABASE_REPLICA_STRATEGY=round_robin
```

Replicas inherit the primary's engine options and become `ReadOnlyDatabase` members named
`replica_0`, `replica_1`, ... alongside `primary`. `DATABASE_READ_REPLICAS` also decides what
the lazily built default database is: with replicas configured it is a `DatabaseCluster`,
otherwise a plain `Database`. See [Database Routing](../routing.md).

## Using the settings objects directly

```python
from sqlargon import Database
from sqlargon.settings import DatabaseSettings

settings = DatabaseSettings(url="sqlite+aiosqlite:///app.db", echo=True)
db = Database(**settings.to_kwargs())
```

`to_kwargs()` returns the keyword arguments for the matching constructor, with unset options
excluded. Both settings classes are ordinary pydantic-settings models, so they can be nested
in an application-wide settings object.

## Connection tracker

`enable_tracker=True` (or `DATABASE_ENABLE_TRACKER=true`) attaches
`sqlargon.tracker.TRACKER` to the engine's pool. It is a test and debugging utility that
records which connections are currently checked out, with the stack trace that opened them:

```python
from sqlargon.tracker import TRACKER

assert not TRACKER.open_connections, TRACKER.open_connections
print(TRACKER.connects, TRACKER.closes)
TRACKER.clear()
```

## OpenTelemetry

When `opentelemetry-instrumentation-sqlalchemy` is installed (the `opentelemetry` extra),
every `Database` instruments its engine automatically on construction — no configuration
required beyond setting up your tracer provider.
