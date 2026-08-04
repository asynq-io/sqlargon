# Dialects and the query builder

Every `Database` picks a `QueryBuilder` from its engine's dialect
(`sqlargon.query_builder.get_query_builder`) and exposes it as `db.query_builder`, or
`repo.qb` on a repository. The builder is what turns repository calls into SQLAlchemy
statements, and it is where dialect-specific SQL — upserts, `RETURNING`, advisory locks —
is isolated.

## Capabilities

Builders declare what they support as an `Option` flag, checked with
`db.query_builder.supports(...)`:

| Dialect | `RETURNING` | `CONFLICTS` | `LOCKS` |
| --- | --- | --- | --- |
| `postgresql` | ✅ | ✅ | ✅ `pg_advisory_lock` |
| `sqlite` | ✅ (SQLite ≥ 3.35) | ✅ | ❌ |
| `mysql` | ✅ | ✅ | ✅ `GET_LOCK` |
| anything else | ❌ | ❌ | ❌ |

```python
from sqlargon.query_builder import Option

if db.query_builder.supports(Option.RETURNING):
    ...
```

Asking for an unsupported feature raises `UnsupportedOption` (a `QueryBuilderError`) when
the statement is built — for instance `return_results=True` on a SQLite build older than
3.35. `RETURNING` is declared for MySQL, but note that only MariaDB 10.5+ implements
`INSERT ... RETURNING`; on MySQL proper the statement will fail at execution time, so keep
`return_results=False` there.

`LOCKS` decides what `db.lock(name)` does: with support it takes a database-native advisory
lock, otherwise it falls back to an `asyncio.Lock` local to the `Database` instance — correct
for tasks sharing that instance, but not across instances or workers.

## Conflict handling per dialect

`insert(..., ignore_conflicts=True)`, `upsert()` and the bulk helpers pass an `OnConflict`
description to the builder, which renders it natively:

| | PostgreSQL | SQLite | MySQL |
| --- | --- | --- | --- |
| ignore | `ON CONFLICT DO NOTHING` | `ON CONFLICT DO NOTHING` | `INSERT IGNORE` |
| update | `ON CONFLICT DO UPDATE` | `ON CONFLICT DO UPDATE` | `ON DUPLICATE KEY UPDATE` |
| `index_elements` | ✅ | ✅ | ignored |
| `constraint` | ✅ | ignored | ignored |
| `index_where` | ✅ | ✅ | ignored |
| `where` | ✅ | ✅ | ignored |
| `set_` / `exclude_set` | ✅ | ✅ | ✅ |

MySQL's `ON DUPLICATE KEY UPDATE` has no target or predicate of its own: it reacts to any
unique key, so `index_elements`, `constraint`, `index_where` and `where` do not apply.
`set_` minus `exclude_set` becomes the assignment list on every dialect.

## Advisory locks

| Dialect | Lock | Release |
| --- | --- | --- |
| `postgresql` | `pg_advisory_lock(:key)` | `pg_advisory_unlock(:key)` |
| `mysql` | `GET_LOCK(:key, -1)` | `RELEASE_LOCK(:key)` |

PostgreSQL advisory locks are keyed by `bigint`, so the lock name is hashed (MD5, modulo
2^63-1) into an integer. Names are therefore global per database — two different names can
in principle collide, and locks are held for the duration of the session the statement runs
on.

## Using the builder directly

The builder is a thin, dialect-aware statement factory. Repositories use it internally, and
it is available for hand-written queries:

```python
qb = db.query_builder

stmt = qb.select(User, with_for_update={"skip_locked": True})
stmt = qb.filter(stmt, User.name == "John", tombstone=False)
result = await db.execute(stmt)

count_stmt = qb.count(User, User.tombstone.is_(False))
page_stmt, total_stmt = qb.page(qb.select(User), offset=0, limit=50, include_total=True)
```

Methods: `select`, `insert`, `update`, `delete`, `filter`, `count`, `page`, `lock`, `unlock`
and `get_lock_pair`. `insert`, `update` and `delete` take `return_results=True` to append a
`RETURNING` clause for the whole table.

## Adding a dialect

Subclass `QueryBuilder`, declare the supported options and override what differs — the base
class already implements every method in portable SQLAlchemy Core:

```python
import sqlalchemy as sa

from sqlargon.query_builder import Option, QueryBuilder


class OracleQueryBuilder(QueryBuilder):
    supported_options = Option.RETURNING

    def lock(self, key: str) -> sa.TextClause: ...
```

`get_query_builder` resolves builders by dialect name and caches them; to use a custom one,
assign it after constructing the database:

```python
db = Database(url="oracle+oracledb://...")
db.query_builder = OracleQueryBuilder()
```

A `DatabaseCluster` takes its builder from the default member and requires every member to
share one dialect, because statements are built before they are routed.
