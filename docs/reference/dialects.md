# Dialects and the query builder

Every `Database` picks a `QueryBuilder` from its engine's dialect
(`sqlargon.query_builder.get_query_builder`) and exposes it as `db.query_builder`, or
`repo.qb` on a repository. The builder is what turns repository calls into SQLAlchemy
statements, and it is where dialect-specific SQL — upserts, `RETURNING`, advisory locks —
is isolated.

## Capabilities

Builders declare what they support as an `Option` flag, checked with
`db.query_builder.supports(...)`:

| Dialect | `RETURNING` | `CONFLICTS` | `LOCKS` | `VECTORS` | `FULL_TEXT` |
| --- | --- | --- | --- | --- | --- |
| `postgresql` | ✅ | ✅ | ✅ `pg_advisory_lock` | ✅ pgvector | ✅ `ts_rank` |
| `sqlite` | ✅ (SQLite ≥ 3.35) | ✅ | ❌ | ✅ sqlite-vector | ❌ |
| `mysql` | ❌ | ✅ | ✅ `GET_LOCK` | ❌ | ❌ |
| anything else | ❌ | ❌ | ❌ | ❌ | ❌ |

```python
from sqlargon.query_builder import Option

if db.query_builder.supports(Option.RETURNING):
    ...
```

Asking for an unsupported feature raises `UnsupportedOption` (a `QueryBuilderError`) when
the statement is built — for instance `return_results=True` on a SQLite build older than
3.35, or on any MySQL family server. MySQL has no `RETURNING` clause at all, and MariaDB
implements it for `INSERT` and `DELETE` but never for `UPDATE`; both reach sqlargon as the
same `mysql` dialect, so neither claims one.

The high-level repository methods do not need the clause. `create`, `create_or_update`,
`get_or_create`, `create_many`, `update_one`, `update_many`, `delete_one` and the bulk
helpers called with `return_results=True` all return models on every dialect: where there
is no `RETURNING` they name the rows they are about to write — generating the client-side
column defaults for the conflict target up front — and read them back in a second
statement, inside the same transaction as the write. Only the low-level
`insert`/`upsert`/`update`/`delete` builders, called with `return_results=True` by hand,
still raise.

The re-read needs to be able to name a row, so a model whose conflict target the *server*
fills in — a sequence, an autoincrement or a server default — raises `UnsupportedOption`
from those methods on such a dialect. Give the column a client-side `default` to fix it.

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
| `qb.excluded(model)` | ✅ | ✅ | ❌ |

MySQL's `ON DUPLICATE KEY UPDATE` has no target or predicate of its own: it reacts to any
unique key, so `index_elements`, `constraint`, `index_where` and `where` do not apply.
`set_` minus `exclude_set` becomes the assignment list on every dialect.

Naming a column in `set_` assigns it the value proposed for insertion. Passing `set_` as a
mapping assigns an expression instead, built against `qb.excluded(model)` — the `excluded`
pseudo row — and the model itself for the stored row. MySQL renders that row inline as
`VALUES(col)` rather than as a table, so it cannot hand it out before the statement exists
and `excluded()` raises `UnsupportedOption` there.

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

The search hooks — `vector_search`, `vector_distance`, `vector_init`, `text_search` and
`rrf_search` — are the same idea for [vector search](../vectors.md): the base class refuses
them with `UnsupportedDialectError`, and the two backends that can search express it in
shapes with nothing in common. PostgreSQL orders by a pgvector operator; SQLite joins the
table valued scan sqlite-vector exposes, because it has no scalar distance function at all.
Keeping both behind one hook is what lets `VectorRepository.search()` be portable.

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
