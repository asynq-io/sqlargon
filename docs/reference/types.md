# Column types and mixins

`sqlargon.types` provides portable column types that compile to the best available construct
per dialect, so the same model definition runs on PostgreSQL, SQLite and MySQL.

## UUIDs

```python
from uuid import UUID

from sqlalchemy.orm import Mapped, mapped_column
from uuid_utils.compat import uuid4

from sqlargon import Base
from sqlargon.types import GUID, GenerateUUID, GenerateUUIDV7


class User(Base):
    id: Mapped[UUID] = mapped_column(
        GUID(),
        primary_key=True,
        default=uuid4,                    # client-side default
        server_default=GenerateUUID(),    # server-side default
        nullable=False,
    )
```

`GUID` stores a native `UUID` on PostgreSQL and `CHAR(36)` elsewhere, and always returns
`uuid.UUID` instances. Strings are accepted when binding.

`GenerateUUID` and `GenerateUUIDV7` are SQL function elements meant for `server_default`:

| Element | PostgreSQL | MySQL | SQLite |
| --- | --- | --- | --- |
| `GenerateUUID` | `GEN_RANDOM_UUID()` | `RANDOM_BYTES`-based v4 expression | `randomblob`-based v4 expression |
| `GenerateUUIDV7` | `uuidv7()`, or `GEN_RANDOM_UUID()` below 18 | `NOW(3)` + `RANDOM_BYTES` v7 expression | same v4 expression as above |

`uuidv7()` is a PostgreSQL 18 builtin. On an older server `GenerateUUIDV7` compiles to
`GEN_RANDOM_UUID()` instead, so the DDL still runs — the default just yields a random v4
value rather than a time-ordered one. The version is read off the connected dialect, so a
dialect that has not seen a server yet (an offline `create_all()` dump, say) compiles the
builtin. The MySQL v7 expression requires MySQL 5.6.4+, and SQLite has no UUID v7
equivalent — it falls back to the random v4 expression too. Where the fallback applies,
rely on the client-side `default=uuid7` if ordering matters.

## Timestamps

```python
from datetime import datetime

from sqlargon.types import Timestamp, now


class Event(Base):
    created_at: Mapped[datetime] = mapped_column(
        Timestamp(), server_default=now(), nullable=False
    )
```

`Timestamp` maps to `TIMESTAMP WITH TIME ZONE` on PostgreSQL, `DATETIME` on SQLite and
`DATETIME(fsp=6)` on MySQL. Naive datetimes are rejected with a `ValueError` on bind;
values are converted to UTC for SQLite and MySQL (which store no offset) and always come
back as UTC-aware datetimes.

`now()` compiles to `CURRENT_TIMESTAMP`, `CURRENT_TIMESTAMP(6)` on MySQL and an equivalent
`strftime` expression on SQLite, and is usable as `server_default`, `onupdate` and
`server_onupdate`.

## JSON

`JSON` maps to `JSONB` on PostgreSQL and to the dialect's JSON type elsewhere, with
`none_as_null=True`. Values are serialized with `orjson`. Its comparator adds portable
operators that compile per dialect:

```python
from sqlargon.types import JSON


class Document(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    tags: Mapped[list] = mapped_column(JSON())
    meta: Mapped[dict] = mapped_column(JSON())


await repo.list(Document.tags.contains(["python", "sql"]))
await repo.list(Document.tags.has_any_key(["python", "rust"]))
await repo.list(Document.tags.has_all_keys(["python", "sql"]))
await repo.list(Document.meta.json_value("owner") == "john")
```

| Operator | PostgreSQL | MySQL | SQLite |
| --- | --- | --- | --- |
| `contains(x)` | `@>` | `JSON_CONTAINS` | `json_each` self-join |
| `has_any_key([...])` | `?\|` | `JSON_CONTAINS_PATH(..., 'one', ...)` | `EXISTS` over `json_each` |
| `has_all_keys([...])` | `?&` | `JSON_CONTAINS_PATH(..., 'all', ...)` | `json_each` self-join |
| `json_value(key)` | `->>` | `JSON_EXTRACT` | `JSON_EXTRACT` |
| `get(key)` | `->` | `JSON_EXTRACT` | `JSON_EXTRACT` |
| `has_key(key)` | `?` | `JSON_CONTAINS_PATH(..., 'one', ...)` | `JSON_TYPE(...) IS NOT NULL` |
| `array_length()` | `JSONB_ARRAY_LENGTH` | `JSON_LENGTH` | `JSON_ARRAY_LENGTH` |
| `keys()` | `JSONB_OBJECT_KEYS` + `JSONB_AGG` | `JSON_KEYS` | `JSON_GROUP_ARRAY` over `json_each` |

!!! warning "`has_any_key` / `has_all_keys` are portable over arrays, not objects"

    Use them to test membership in a JSON **array** — that is the one meaning all three
    dialects agree on. Against a JSON **object** they diverge: PostgreSQL and MySQL test the
    object's *keys*, while the SQLite fallback tests the *values* produced by `json_each`.
    To test a key of an object portably, use `has_key(key)`, which addresses object keys on
    every dialect.

### Mutating a document server-side

The mutation operators rewrite a document in the `UPDATE` itself, so a single key can be
changed without reading the row into Python and writing it back — no lost update, one
round trip:

```python
await repo.update({Document.meta: Document.meta.set_key("owner", "john")}).execute()
await repo.update({Document.meta: Document.meta.update({"owner": "john", "hits": 0})}).execute()
await repo.update({Document.meta: Document.meta.remove_key("owner")}).execute()
```

| Operator | PostgreSQL | MySQL | SQLite |
| --- | --- | --- | --- |
| `set_key(key, value)` | `\|\|` | `JSON_SET` | `JSON_SET` |
| `update({...})` | `\|\|` | `JSON_SET` | `JSON_SET` |
| `remove_key(*keys)` | `-` over `text[]` | `JSON_REMOVE` | `JSON_REMOVE` |
| `insert_key(key, value)` | `\|\|`, patch on the left | `JSON_INSERT` | `JSON_INSERT` |
| `replace_key(key, value)` | `JSONB_SET(..., false)` | `JSON_REPLACE` | `JSON_REPLACE` |
| `array_append(value)` | `\|\|` + `JSONB_BUILD_ARRAY` | `JSON_ARRAY_APPEND` | `JSON_INSERT(..., '$[#]', ...)` |

`insert_key` only writes a key that is **absent**; `replace_key` only one already
**present**. Every mutation returns a JSON expression, so they nest:

```python
Document.meta.update({"c": 3}).remove_key("a")
```

!!! warning "What the mutation operators do not smooth over"

    - **`NULL` in, `NULL` out.** `JSONB_SET` and `JSON_SET` both return `NULL` for a `NULL`
      document, and these operators match that rather than coalescing to `{}`. Give the
      column a `server_default` of `'{}'` if you need a document to always be there.
    - **Objects only.** The `JSON_SET` family addresses `$."key"`, so `set_key`,
      `update`, `remove_key`, `insert_key` and `replace_key` assume the document is an
      object. Use `array_append` for arrays.
    - **Top-level keys only.** There are no nested paths or array indices; a key is always
      one level down.
    - **`update` is a shallow merge.** A top-level key is replaced wholesale, not merged
      into recursively — the semantics of PostgreSQL's `||`. Deep merge-patch
      (`JSON_MERGE_PATCH`, `json_patch`) is deliberately absent: PostgreSQL has no builtin
      for it.
    - **`array_length` is portable over arrays only.** Given an object PostgreSQL raises,
      SQLite answers 0 and MySQL answers 1.

The underlying function elements — `json_contains`, `json_has_any_key`, `json_has_all_keys`,
`json_value`, `json_get`, `json_has_key`, `json_array_length`, `json_keys`, `json_update`,
`json_set_key`, `json_remove_key`, `json_insert_key`, `json_replace_key` and
`json_array_append` — are importable from `sqlargon.types.json` for use outside a `JSON`
column. The key operators require string keys and raise `ValueError` otherwise.

## Pydantic-validated columns

`sqlargon.types.pydantic` stores validated objects in a JSON column and hydrates them back
on load:

```python
from pydantic import BaseModel

from sqlargon.types.pydantic import Pydantic, ValidatedType


class Address(BaseModel):
    city: str
    zip_code: str


class Customer(Base):
    id: Mapped[int] = mapped_column(primary_key=True)
    address: Mapped[Address] = mapped_column(Pydantic(Address))
    scores: Mapped[list[int]] = mapped_column(ValidatedType(list[int]))
```

`Pydantic(model)` accepts either a model instance or a dict on write and always returns a
model instance on read. `ValidatedType(type_)` does the same for any type a
`pydantic.TypeAdapter` can handle; pass `validate=False` to skip validation on write.
Both accept `sa_column_type=` to store in something other than `JSON` (e.g. `sa.Text`).

## Mixins

`sqlargon.mixins` bundles the types above into reusable column sets:

| Mixin | Columns | Extras |
| --- | --- | --- |
| `UUIDModelMixin` | `id` — `GUID` primary key, `uuid4` client default, `GenerateUUID()` server default | |
| `UUIDV7ModelMixin` | `id` — `GUID` primary key, `uuid7` client default, `GenerateUUIDV7()` server default | |
| `CreatedUpdatedMixin` | `created_at`, `updated_at` — `Timestamp`, `now()` server defaults, `onupdate` | `is_new` hybrid property |
| `SoftDeleteMixin` | `tombstone` — boolean, defaults to false | `not_deleted` and `is_deleted` hybrid properties |
| `VersionedMixin` | *(abstract marker — no columns)* | |
| `UUIDVersionedMixin` | `version_id` — `GUID`, `uuid4` default, `GenerateUUID()` server default | `__mapper_args__` with `version_id_col` + UUID generator |
| `XminVersionedMixin` | `xmin` — PostgreSQL system column, `String`, `system=True`, `FetchedValue()` | `__mapper_args__` with `version_id_col` + `version_id_generator=False` |
| `AuditableMixin` | *(abstract marker — no columns; extends `SoftDeleteMixin` and `VersionedMixin`)* | `audit_key()`, `latest_version()`, `is_latest()` |
| `IntegerAuditableMixin` | `version` — `Integer` primary key, starting at 1 | successor is `version + 1` |
| `UUIDAuditableMixin` | `version` — `GUID` primary key, `uuid7` default, `GenerateUUIDV7()` server default | successor is a fresh UUIDv7 |

```python
from sqlargon.mixins import CreatedUpdatedMixin, SoftDeleteMixin, UUIDV7ModelMixin


class User(UUIDV7ModelMixin, CreatedUpdatedMixin, SoftDeleteMixin, Base):
    name: Mapped[str] = mapped_column(sa.Unicode(255))
```

Both timestamps come from the database rather than from Python: they have no client-side
default, so a single evaluation of `now()` per statement fills them, and `updated_at` is
moved on by `onupdate` / `server_onupdate` — rows updated by raw SQL keep an accurate
timestamp too. Because the two columns share one server-side value, a freshly inserted row
has `created_at == updated_at` exactly, including every row of a multi-row insert.

The hybrids work at instance level and as SQL expressions, so either can be used in a
filter:

```python
await repo.list(User.is_new)        # created_at = updated_at
await repo.list(User.not_deleted)   # NOT tombstone
await repo.list(User.is_deleted)    # tombstone IS true

user = await repo.get(id=user_id)
user.is_new, user.not_deleted, user.is_deleted
```

`SoftDeleteMixin` only declares the column; to have deletes raise the flag automatically,
build the repository on [`SoftDeleteRepository`](../usage.md#soft-deletes) and the model on
`SoftDeleteBase`, which is that mixin already combined with `Base`:

```python
from sqlargon import SoftDeleteBase


class User(UUIDV7ModelMixin, CreatedUpdatedMixin, SoftDeleteBase):
    name: Mapped[str] = mapped_column(sa.Unicode(255))
```

`SoftDeleteModel` is the matching type variable, bound to `SoftDeleteBase`.

`VersionedMixin` is an abstract marker — use one of its concrete subclasses:

- `UUIDVersionedMixin` — backend-agnostic, a `GUID` version column with a fresh UUID on
  every update. `VersionedBase` combines it with `Base`:

```python
from sqlargon import VersionedBase


class User(UUIDV7ModelMixin, VersionedBase):
    name: Mapped[str] = mapped_column(sa.Unicode(255))
```

- `XminVersionedMixin` — PostgreSQL only, uses the `xmin` system column (server-managed,
  changes on every UPDATE). `XminVersionedBase` combines it with `Base`:

```python
from sqlargon import XminVersionedBase


class User(UUIDV7ModelMixin, XminVersionedBase):
    name: Mapped[str] = mapped_column(sa.Unicode(255))
```

Both set `__mapper_args__` with `version_id_col`, enabling SQLAlchemy's ORM-level
versioning when using `AsyncSession` directly. `VersionedModel` is the matching type
variable, bound to `VersionedBase`. See [Versioned models](../usage.md#versioned-models)
for the repository API.

`AuditableMixin` is an abstract marker too — use `IntegerAuditableMixin` or
`UUIDAuditableMixin`, or the bases combining them with `Base`:

```python
from sqlargon import AuditableBase, UUIDAuditableBase


class Article(UUIDModelMixin, AuditableBase):  # versions 1, 2, 3 ...
    title: Mapped[str] = mapped_column(sa.Unicode(255))


class Draft(UUIDModelMixin, UUIDAuditableBase):  # UUIDv7 versions
    title: Mapped[str] = mapped_column(sa.Unicode(255))
```

`AnyAuditableBase` is the abstract base both share and `AuditableModel` the matching type
variable. Pair either with [`AuditableRepository`](../auditable.md), which appends a new
version instead of updating a row.
