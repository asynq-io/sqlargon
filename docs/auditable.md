# Auditable models

An auditable repository never updates a row. Every change appends a new row carrying the
next `version` of the same entity, so the table *is* the audit log — the history is the
data, not a copy of it in a side table that can drift.

Reads are scoped to the newest live version, so the usual repository methods keep their
usual meaning and the history stays underneath, one method away.

## Declaring the model

Inherit `AuditableBase` and combine it with whatever identifies the entity — usually
`UUIDModelMixin`:

```python
import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon import AuditableBase, AuditableRepository
from sqlargon.mixins import UUIDModelMixin


class Article(UUIDModelMixin, AuditableBase):
    title: Mapped[str] = mapped_column(sa.Unicode(255))
    body: Mapped[str | None] = mapped_column(sa.Text, nullable=True)


class ArticleRepository(AuditableRepository[Article]): ...
```

The version column joins the primary key, so `Article` is keyed by `(id, version)` and its
**entity key** is derived as everything in the primary key except the version — here
`(id,)`. Nothing else to configure.

`AuditableBase` also brings `created_at` / `updated_at` and the `tombstone` column, so the
model carries when each version was written and which one records a deletion. Because a row
is never updated, `updated_at` always equals `created_at`.

!!! tip "Put the entity key in a mixin"
    Columns are ordered by when they were declared, so an entity key declared in the model
    body lands *after* `version` in the primary key and its index. Taking the key from a
    mixin — `UUIDModelMixin` above — keeps the index `(id, version)`, which is the order
    every latest-version read wants.

## Writing

Every write appends. `create` starts an entity at version 1, and each subsequent write adds
the next version:

```python
articles = ArticleRepository()

article = await articles.create(title="draft")  # version 1
await articles.update_one({"title": "revised"}, Article.id == article.id)  # version 2
await articles.update_one({"title": "final"}, Article.id == article.id)  # version 3
```

A column the write does not name is carried forward from the version it supersedes, so an
append is a change to the entity, not a replacement of it.

`update()` and `delete()` still build a statement, so the fluent form works unchanged — it
compiles to an `INSERT ... SELECT` reading the current heads and writing their successors,
which makes an append one statement rather than a read followed by a write:

```python
await articles.update({"title": "final"}).filter(Article.id == article.id)
```

`create_or_update` appends to an entity that exists and creates one that does not;
`bulk_create_or_update` is its bulk form, and `bulk_update` appends a different set of
values per entity:

```python
await articles.create_or_update(id=article.id, title="fourth")
await articles.bulk_create_or_update(
    [{"id": article.id, "title": "fifth"}, {"id": uuid4(), "title": "brand new"}]
)
await articles.bulk_update([{"id": a_id, "title": "a"}, {"id": b_id, "title": "b"}])
```

The one statement an append-only table cannot serve is `upsert()`, whose whole meaning is
"resolve a conflict by rewriting the conflicting row". It raises `AppendOnlyError` and
points at the two methods above.

## Deleting

Deletion appends a tombstoned version rather than removing anything, so `remove`,
`delete_one` and `delete_many` are all recoverable:

```python
await articles.remove(Article.id == article.id)  # appends version 4, tombstoned

await articles.list()  # the entity is gone from reads
await articles.versions().count()  # but all four rows are still there

await articles.restore(Article.id == article.id)  # appends version 5, live again
```

## Reading

| Call | Returns |
| --- | --- |
| `list()` / `get()` / `first()` | the newest live version of each entity |
| `count()` | how many **entities** there are |
| `versions()` | a copy covering every version of every entity |
| `versions().count()` | how many **rows** there are |
| `history(...)` | every version of the matched entities, oldest first |
| `get_version(n, ...)` | one exact version, tombstoned or not |
| `at(timestamp)` | a copy reading the state as it stood at that moment |
| `with_deleted()` | the newest version even when it is a tombstone |
| `only_deleted()` | entities whose newest version is a tombstone |

```python
await articles.get(id=article.id)  # version 3
await articles.history(id=article.id)  # versions 1, 2 and 3
await articles.get_version(2, id=article.id)  # version 2

yesterday = utc_now() - timedelta(days=1)
await articles.at(yesterday).list()  # the state as of yesterday
```

`at()` resolves each entity to the newest version recorded up to that moment, and keeps an
entity tombstoned by then hidden — exactly as it would have been at the time.

The latest-version scope is a correlated subquery, available on the model itself as
`Article.is_latest()`, so it composes into any query of your own:

```sql
SELECT ... FROM article
WHERE article.version = (SELECT v.version FROM article v
                         WHERE v.id = article.id
                         ORDER BY v.version DESC LIMIT 1)
  AND NOT article.tombstone
```

## Concurrency

Because the version is part of the primary key, two writers deriving the same successor
collide there rather than one of them silently winning — the loser gets an `IntegrityError`
instead of losing its append.

`update_if_match` and `delete_if_match` are inherited from
[`VersionedRepository`](usage.md#versioned-models) and land on the append path, giving the
cheaper check first:

```python
from sqlargon import ConcurrentModificationError

article = await articles.get(id=article_id)
try:
    await articles.update_if_match(
        {"title": "final"},
        Article.id == article_id,
        expected_version=article.version,
        raise_on_mismatch=True,
    )
except ConcurrentModificationError:
    ...  # someone appended a version first
```

## Versioning strategies

| Base | Version column | Successor |
| --- | --- | --- |
| `AuditableBase` | `Integer`, starting at 1 | `version + 1`, in SQL |
| `UUIDAuditableBase` | `GUID`, UUIDv7 | a fresh UUIDv7, minted without reading the current one |

Pick `AuditableBase` for a human-readable document version — 1, 2, 3 — and for strict
chronological ordering. Pick `UUIDAuditableBase` when writers cannot coordinate on a
counter: a UUIDv7 is time-sortable, so the newest version is still the greatest one.

!!! warning "UUIDv7 ordering across processes"
    `uuid7()` is monotonic within a process, but two processes appending in the same
    millisecond can produce an inverted pair, which would make the older of the two look
    newest. Use `AuditableBase` where that matters.

## Relationships

A row of an auditable model is one *version* of an entity, so a reference to it has to say
which version it means. `sqlargon.audit` covers both answers.

### Pinned to an exact version

The child stores the entity key and the version, under a real composite foreign key. That
makes it an ordinary many-to-one — writable, and joined without a `primaryjoin`:

```python
from sqlargon.audit import version_foreign_key, version_mapped_column


class Comment(UUIDModelMixin, Base):
    article_id: Mapped[UUID] = mapped_column(GUID())
    article_version = version_mapped_column(Article)
    body: Mapped[str] = mapped_column(sa.Text)

    __table_args__ = (
        version_foreign_key(Article, "article_id", "article_version"),
    )

    article: Mapped[Article] = relationship()
```

`version_mapped_column` types itself from the parent, so the child never has to know which
versioning strategy it uses. The comment keeps pointing at the version it was written
against, however far the article moves on.

### Following the latest version

The child stores only the entity key. There can be no foreign key — the parent's primary
key holds a version this child deliberately does not pin — so the relationship carries the
latest predicate in its join and is necessarily `viewonly`:

```python
from sqlargon.audit import latest_relationship


class Bookmark(UUIDModelMixin, Base):
    article_id: Mapped[UUID] = mapped_column(GUID())

    article: Mapped[Article] = latest_relationship(Article, "article_id")
```

Both work under `selectinload`, which the repository's `load()` uses:

```python
await comments.load(Comment.article).all()
await bookmarks.load(Bookmark.article).all()
```

Pass `uselist=True` for a collection.

## Retention

`purge()` is the only method that destroys history — for retention, not for deletion, which
`remove` records instead. It physically deletes every superseded version and keeps the
newest:

```python
await articles.purge(id=article.id)
await articles.purge(Article.created_at < cutoff)
```

A version a `version_foreign_key` still points at is protected by that key: purging it
raises rather than orphaning the child. Declare the key `ondelete="CASCADE"` if you would
rather the children went with it.
