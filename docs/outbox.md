# Outbox

`sqlargon.outbox` implements the transactional outbox pattern: a write through
a repository also appends a [CloudEvent](https://cloudevents.io)-shaped row to
the `outbox_events` table **in the same transaction**, and a background relay
publishes those rows to a broker. Because the row and its event commit or roll
back together, an event can neither be lost by a rollback nor published for a
row that never committed.

It requires the `sqlargon[outbox]` extra (`anyio`, for the relay).

```python
import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon import Base
from sqlargon.mixins import CreatedUpdatedMixin, UUIDModelMixin
from sqlargon.outbox import OutboxConfig, OutboxRelay, OutboxRepository


class User(UUIDModelMixin, CreatedUpdatedMixin, Base):
    name: Mapped[str] = mapped_column(sa.Unicode(255))
    password: Mapped[str] = mapped_column(sa.Unicode(255))


class UserRepository(OutboxRepository[User]):
    outbox = OutboxConfig(topic="users", exclude={"password"})


await UserRepository().create(name="John", password=hashed)
# -> one outbox_events row, type "user.created", password left out
```

sqlargon never talks to a broker itself. The relay takes a *publisher* — any
`async def publish(event: OutboxEvent) -> None` — so any client will do; see
[eventiq](#eventiq) for the adapter that ships with it.

## Configuring a repository

The `OutboxConfig` assigned to the repository's `outbox` attribute decides what
its writes look like as events. The model must carry `CreatedUpdatedMixin` —
its `is_new` is what tells an insert from an update — but nothing beyond that:
no declarative base of its own, and whether a write is recorded is decided by
the repository it goes through, the same way `on_conflict` is. So two
repositories over the same model can publish differently — useful when one of
them serves a less trusted consumer:

```python
class PublicUserRepository(OutboxRepository[User]):
    outbox = OutboxConfig(topic="users.public", include=frozenset({"id", "name"}))
```

The default is a bare `OutboxConfig()`, so a repository that configures nothing
records every write with the table name as topic and type prefix.

| Field | Meaning |
| --- | --- |
| `topic` | The CloudEvents `subject`. Defaults to the table name. |
| `type_prefix` | Prefix of the CloudEvents `type`. Defaults to the table name. |
| `source` | The CloudEvents `source`. Defaults to `None`, leaving it to the relay. |
| `exclude` | Columns kept out of the payload. |
| `include` | The payload columns outright; wins over `exclude`. |
| `operations` | Which writes are recorded at all. |
| `attributes` | Extra CloudEvents attributes, next to the payload rather than inside it. |

The event `type` is `f"{type_prefix}.{operation}"`, so the default gives
`user.created`, `user.updated` and `user.deleted`.

Restricting the operations recorded, for a table whose updates nobody cares
about:

```python
from sqlargon.outbox import Operation

OutboxConfig(operations=frozenset({Operation.CREATED, Operation.DELETED}))
```

The topic, the event type of each recorded operation and the payload columns
are derived from the config, and readable for inspection as `repository.topic`,
`repository.event_types` and `repository.payload_columns`.

### Extra attributes

Some values belong *next to* the payload rather than inside it — a `tenant_id`
a broker routes on, the trace of the request that caused the write. `attributes`
names them, and they are stored in the event's `attributes` column and
published as top-level CloudEvents attributes:

```python
from contextvars import ContextVar

traceparent: ContextVar[str] = ContextVar("traceparent")


class UserRepository(OutboxRepository[User]):
    outbox = OutboxConfig(
        topic="users",
        exclude={"password", "tenant_id"},
        attributes={
            "tenant_id": "tenant_id",  # an attribute of the written row
            "traceparent": lambda _: traceparent.get(),  # anything else
        },
    )
```

A `str` names an attribute of the written row — a column, or any other
attribute of the model — and a callable is handed the row and returns the
value, which is how a `ContextVar` is read. Either way the value is read
**when the write happens**, not when the event is published: the relay runs in
a background task, long after the request context the write ran in is gone.

A column can be excluded from the payload and promoted to an attribute at
once, as `tenant_id` is above. Values are reduced to JSON primitives the way
the payload is, so a `UUID` is stored as a string and parsed back by the
CloudEvent class that declares it.

Names of CloudEvents core attributes (`id`, `type`, `source`, `subject`,
`time`, `data`, …) are rejected: every event carries those of its own.

## What is recorded

Every write through the repository is recorded, one event per affected row:

| Method | Operation |
| --- | --- |
| `create`, `bulk_create`, `create_many`, `get_or_create` | `created` |
| `create_or_update`, `bulk_create_or_update` | `created` or `updated`, per row |
| `update_one`, `update_many`, `bulk_update` | `updated` |
| `delete_one`, `delete_many`, `remove` | `deleted` |

The payload is the row after the write (before it, for a delete), reduced to
JSON primitives — UUIDs and timestamps become strings — and keyed by column
name.

An upsert is not reported as a kind of its own: each written row is recorded
as what it turned out to be. `CreatedUpdatedMixin` gives `created_at` and
`updated_at` one shared timestamp per statement, so a row still carrying it —
`is_new` — was inserted, and one whose `updated_at` has moved on was updated.
A single `bulk_create_or_update()` that inserts some rows and updates others
therefore records `created` for the first and `updated` for the second. This
is why the mixin is required, and why an upsert through *any* repository
leaves `created_at` alone: a row was created when it was created, and
rewriting it would make an updated row look new.

Building the events needs the written rows back, so `remove()` and
`delete_many()` use a RETURNING statement here instead of a bare `DELETE`, and
`bulk_update()` reads its rows back by the keys it matched on. On MySQL and
MariaDB, which have no `RETURNING`, the repository's usual fallback (select the
identities, write, re-fetch) supplies them, so nothing dialect-specific is
involved — the outbox works on every backend sqlargon supports.

Only writes that go *through the repository* are recorded. Raw SQL, a
migration or another service writing to the table leaves no event behind.

!!! note "Combining with soft delete"
    `SoftDeleteRepository` rewrites `delete()` into an update raising the
    tombstone. Combining the two repositories records a `deleted` event for a
    soft delete, which is what the caller asked for, but the statement behind
    it is an `UPDATE` — a consumer replaying the raw payload sees the row is
    still there, with `tombstone` set.

## Running the relay

`OutboxRelay` polls for unpublished events and hands them to the publisher.
Use it as a long-running entrypoint, or start it in the background:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from sqlargon.outbox import OutboxEvent, OutboxRelay


async def publish(event: OutboxEvent) -> None:
    await broker.send(event.topic, event.data)


relay = OutboxRelay(publish)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with relay.running():  # yields once the relay is live
        yield


app = FastAPI(lifespan=lifespan)
```

`run()` also supports anyio's initialization barrier, so it can be embedded in
an existing task group with `await tg.start(relay.run)`.

Delivery semantics:

- Events are published **one at a time, in the order they were written**
  (`created_at`, then `id`). An outbox that reorders its events is not much of
  an outbox.
- A publisher that raises stops the batch, so a broker outage delays the
  events behind the failed one rather than letting them overtake it. The
  failure is logged (logger `sqlargon.outbox.relay`), recorded in
  `last_error`, and retried with an exponential backoff.
- Delivery is **at least once**: an event published just before the process
  dies is published again, because it had not been marked yet. Consumers must
  be idempotent — the CloudEvents `id` is stable across redeliveries.
- After `max_attempts` failures an event stops being claimed and keeps its
  `last_error` for inspection.
- A failing poll (a dropped connection, say) is logged and retried on the next
  tick; the relay keeps running.

`dispatch_once()` runs a single claim-and-publish pass and returns how many
events it published — handy in tests, or to drain the table from a script.

### Multiple relays

A claim locks the due rows with `SELECT ... FOR UPDATE SKIP LOCKED` and leases
them — `available_at` is pushed `lease` into the future and `attempts` bumped —
within the same transaction. Relays polling concurrently skip locked rows, and
a relay that dies mid-batch leaves nothing wedged: its lease expires and
another picks the events up.

## Retention

Published events are marked, not deleted, so the table doubles as an audit
trail. Deleting the ones older than `retention` (7 days by default) is
`purge()`, which the relay never runs on its own — polling and pruning keep
different schedules, and nothing is dropped unless you ask for it:

```python
relay = OutboxRelay(publish, retention=timedelta(days=30))
cron.task("0 3 * * *", "purge_outbox", relay.purge)
```

`purge()` takes no arguments and returns how many rows it deleted, so it works
as a cron task as it stands. It only ever deletes rows that have been
published. Skip registering it to keep every event.

## Configuration

`OutboxRelay` takes:

- `publisher` — the coroutine function each claimed event is handed to.
- `repository` — the `OutboxEventRepository` to read events with; defaults to
  a fresh one bound to the process-wide default database. Pass
  `OutboxEventRepository().using(db=other_database)` to relay another
  database.
- `poll_interval` — seconds between polls (default `1.0`).
- `batch_size` — maximum events claimed per poll (default `100`).
- `lease` — how long a claimed event stays invisible to other relays
  (default 5 minutes).
- `max_attempts` — attempts after which an event is left alone (default `10`).
- `retry_backoff` / `max_retry_delay` — base and ceiling, in seconds, of the
  exponential retry delay (defaults `2.0` and `300.0`).
- `retention` — how much history `purge()` keeps (default 7 days).

## eventiq

`sqlargon.integrations.eventiq` converts an event row into an
`eventiq.CloudEvent` and publishes it through a `Service`. It is the only
module that imports `eventiq`, so the dependency stays optional — install the
`sqlargon[eventiq]` extra to use it.

```python
from sqlargon.integrations.eventiq import eventiq_publisher
from sqlargon.outbox import OutboxRelay

relay = OutboxRelay(eventiq_publisher(service, source="orders"))
```

The mapping is direct:

| `OutboxEvent` | `CloudEvent` |
| --- | --- |
| `id` | `id` |
| `created_at` | `time` |
| `topic` | `topic` (serialized as `subject`) |
| `type` | `type` |
| `source`, or the fallback given to the publisher | `source` |
| `data` | `data` |
| `attributes` | top-level attributes, one per key |
| `headers` | the broker's transport headers |

`to_cloud_event(event, source=...)` does the conversion on its own, for code
that publishes through something other than a relay.

### Publishing your own CloudEvent class

Services usually publish a base class of their own rather than the plain
`CloudEvent` — one that declares the [extra attributes](#extra-attributes)
every event of theirs carries. Name it with `event_class` and the relay builds
and validates that class instead:

```python
from uuid import UUID

from eventiq import CloudEvent


class TenantEvent(CloudEvent):
    tenant_id: UUID


relay = OutboxRelay(eventiq_publisher(service, event_class=TenantEvent))
```

The row's `attributes` are passed to it as top-level attributes, so the
`tenant_id` stored as a string comes back parsed as a `UUID`, and an event
missing an attribute the class requires fails validation rather than being
published half-formed. The core CloudEvents attributes always come from the
row's own columns, so nothing stored in `attributes` can shadow them.
`to_cloud_event(event, event_class=TenantEvent)` returns the same class,
typed as such.

## The outbox_events table

Events are stored in the `outbox_events` table, registered on the shared
`Base` metadata when `sqlargon.outbox` is imported — `db.create_all()` creates
it, and an Alembic autogenerate pass picks it up (see
[Alembic Migrations](migrations.md)).

| Column | Purpose |
| --- | --- |
| `id` | The CloudEvents `id`. |
| `created_at` | The CloudEvents `time`, and the dispatch order. |
| `topic`, `type`, `source` | The CloudEvents routing attributes. |
| `data` | The row snapshot, as JSON. |
| `attributes` | The extra CloudEvents attributes, as JSON. |
| `headers` | The broker's transport headers, as JSON. |
| `published_at` | `NULL` until published; set on success. |
| `available_at` | Not before which the event may be claimed. |
| `attempts` | Claims so far, used by `max_attempts`. |
| `last_error` | Why the last attempt failed. |

Every column carries a server default as well as a client one, so a row
inserted without naming them is still complete.

`OutboxEventRepository` exposes the table directly, for monitoring or for a
dispatcher of your own: `pending_count()`, `claim_pending()`,
`mark_published()`, `mark_failed()` and `purge()`.
