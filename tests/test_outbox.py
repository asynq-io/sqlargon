"""Unit tests for OutboxRepository, OutboxEventRepository and OutboxRelay.

Mirrors ``test_versioned.py``: in-memory SQLite via the shared ``db``
fixture, module-level models to survive ``--count=3`` re-registration.
"""

from contextvars import ContextVar
from datetime import timedelta
from uuid import uuid4

import anyio
import pytest
import sqlalchemy as sa

from sqlargon import Base, Database, atomic
from sqlargon.mixins import CreatedUpdatedMixin, UUIDModelMixin
from sqlargon.outbox import (
    Operation,
    OutboxConfig,
    OutboxEvent,
    OutboxEventRepository,
    OutboxRelay,
    OutboxRepository,
)
from sqlargon.types import GUID
from sqlargon.typing import OnConflictOptions
from sqlargon.utils import utc_now

HASHED = "not-a-real-secret"

traceparent: ContextVar[str] = ContextVar("traceparent", default="none")

# Models defined at module level to avoid re-registration with --count=3


class OutboxUser(UUIDModelMixin, CreatedUpdatedMixin, Base):
    __tablename__ = "test_outbox_user"

    name = sa.Column(sa.Unicode(255), nullable=True)
    password = sa.Column(sa.Unicode(255), nullable=True)
    tenant_id = sa.Column(GUID(), nullable=True)


class InsertOnlyNote(UUIDModelMixin, CreatedUpdatedMixin, Base):
    __tablename__ = "test_outbox_note"

    body = sa.Column(sa.Unicode(255), nullable=True)


class OutboxTag(UUIDModelMixin, CreatedUpdatedMixin, Base):
    """A model with a unique key, so a conflicting insert really writes nothing."""

    __tablename__ = "test_outbox_tag"
    __table_args__ = (sa.UniqueConstraint("label"),)

    label = sa.Column(sa.Unicode(255), nullable=False)


class BareModel(UUIDModelMixin, CreatedUpdatedMixin, Base):
    __tablename__ = "test_outbox_bare"


class PlainModel(CreatedUpdatedMixin, Base):
    __tablename__ = "test_outbox_plain"
    id = sa.Column(sa.Integer, primary_key=True)


class UntimestampedModel(UUIDModelMixin, Base):
    """A model the outbox cannot tell an insert from an update on."""

    __tablename__ = "test_outbox_untimestamped"


class OutboxUserRepository(OutboxRepository[OutboxUser]):
    default_order_by = OutboxUser.name

    outbox = OutboxConfig(
        topic="users", type_prefix="user", source="tests", exclude={"password"}
    )


class BareRepository(OutboxRepository[BareModel]):
    """The repository with no ``outbox`` of its own."""


class NarrowedUserRepository(OutboxUserRepository):
    """Tightens the exclusions further, the way ``on_conflict`` is."""

    outbox = OutboxConfig(topic="users", include=frozenset({"name"}))


class TenantUserRepository(OutboxRepository[OutboxUser]):
    """Promotes a column and a context variable to CloudEvent attributes."""

    outbox = OutboxConfig(
        topic="users",
        type_prefix="user",
        exclude={"password", "tenant_id"},
        attributes={
            "tenant_id": "tenant_id",
            "traceparent": lambda _: traceparent.get(),
        },
    )


class NoteRepository(OutboxRepository[InsertOnlyNote]):
    outbox = OutboxConfig(operations=frozenset({Operation.CREATED}))


class TagRepository(OutboxRepository[OutboxTag]):
    @property
    def on_conflict(self) -> OnConflictOptions:
        return {"index_elements": {"label"}, "set_": {"label"}}


class EventRepository(OutboxEventRepository):
    pass


# --- fixtures ---


@pytest.fixture(autouse=True)
async def tables(db: Database):
    created = (
        OutboxUser.__table__,
        InsertOnlyNote.__table__,
        OutboxTag.__table__,
        OutboxEvent.__table__,
    )
    async with db.engine.begin() as conn:
        for table in created:
            await conn.run_sync(table.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        for table in reversed(created):
            await conn.run_sync(table.drop, checkfirst=True)


@pytest.fixture
def users():
    return OutboxUserRepository()


@pytest.fixture
def tenant_users():
    return TenantUserRepository()


@pytest.fixture
def notes():
    return NoteRepository()


@pytest.fixture
def tags():
    return TagRepository()


@pytest.fixture
def events():
    return EventRepository()


async def stored(events: EventRepository) -> list[OutboxEvent]:
    return list(await events.select().order_by(OutboxEvent.created_at).all())


# --- configuration ---


def payload_of(repository: OutboxRepository) -> tuple[str, ...]:
    return tuple(name for name, _ in repository.payload_columns)


def test_any_timestamped_model_can_be_recorded():
    """Nothing else marks a model as recorded -- the repository decides."""

    class Repository(OutboxRepository[PlainModel]):
        pass

    assert Repository.model is PlainModel


def test_a_model_without_timestamps_is_rejected():
    with pytest.raises(TypeError, match="CreatedUpdatedMixin"):

        class Repository(OutboxRepository[UntimestampedModel]):
            pass


def test_a_repository_needs_no_configuration():
    assert isinstance(BareRepository().outbox, OutboxConfig)
    assert BareRepository().topic == "test_outbox_bare"


def test_topic_and_type_default_to_the_table_name():
    notes = NoteRepository()

    assert notes.topic == "test_outbox_note"
    assert notes.event_types[Operation.CREATED] == "test_outbox_note.created"


def test_repository_config_names_the_topic_and_type(users):
    assert users.topic == "users"
    assert users.event_types[Operation.DELETED] == "user.deleted"


def test_excluded_columns_are_kept_out_of_the_payload(users):
    assert "password" not in payload_of(users)
    assert "name" in payload_of(users)


def test_include_wins_over_exclude():
    class Repository(OutboxRepository[OutboxUser]):
        outbox = OutboxConfig(include=frozenset({"name"}), exclude=frozenset({"name"}))

    assert payload_of(Repository()) == ("name",)


def test_only_declared_operations_get_an_event_type():
    types = NoteRepository().event_types

    assert Operation.CREATED in types
    assert Operation.DELETED not in types


def test_the_config_belongs_to_the_class(users):
    assert users.outbox is OutboxUserRepository.outbox


def test_a_subclass_overrides_the_config_of_its_base():
    assert NarrowedUserRepository().outbox is not OutboxUserRepository.outbox
    assert payload_of(NarrowedUserRepository()) == ("name",)
    assert "password" not in payload_of(OutboxUserRepository())


def test_attributes_cannot_shadow_the_core_ones():
    with pytest.raises(ValueError, match="source, type"):
        OutboxConfig(attributes={"type": "name", "source": "name", "tenant": "name"})


# --- capture ---


async def test_create_records_an_event(users, events):
    user = await users.create(name="John", password=HASHED)

    (event,) = await stored(events)
    assert event.topic == "users"
    assert event.type == "user.created"
    assert event.source == "tests"
    assert event.data["name"] == "John"
    assert event.data["id"] == str(user.id)
    assert "password" not in event.data
    assert event.published_at is None
    assert event.attempts == 0


async def test_an_upsert_that_updates_records_an_update(users, events):
    user = await users.create(name="John")
    await users.create_or_update(id=user.id, name="Jane")

    types = [event.type for event in await stored(events)]
    assert types == ["user.created", "user.updated"]


async def test_an_upsert_that_inserts_records_a_create(users, events):
    await users.create_or_update(id=uuid4(), name="John")

    assert [event.type for event in await stored(events)] == ["user.created"]


async def test_one_upsert_records_what_each_row_turned_out_to_be(users, events):
    existing = await users.create(name="John")

    await users.bulk_create_or_update(
        [{"id": existing.id, "name": "Jane"}, {"id": uuid4(), "name": "Jack"}]
    )

    recorded = {event.data["name"]: event.type for event in await stored(events)}
    assert recorded["Jane"] == "user.updated"
    assert recorded["Jack"] == "user.created"


async def test_an_upsert_leaves_the_creation_time_alone(users):
    user = await users.create(name="John")

    updated = await users.create_or_update(id=user.id, name="Jane")

    assert updated.created_at == user.created_at
    assert updated.updated_at > updated.created_at


async def test_update_one_records_an_update(users, events):
    user = await users.create(name="John")
    await users.update_one({"name": "Jane"}, id=user.id)

    event = (await stored(events))[-1]
    assert event.type == "user.updated"
    assert event.data["name"] == "Jane"


async def test_update_many_records_one_event_per_row(users, events):
    await users.create_many([{"name": "a"}, {"name": "b"}])
    await users.update_many({"password": HASHED}, OutboxUser.name.in_(["a", "b"]))

    updated = [e for e in await stored(events) if e.type == "user.updated"]
    assert sorted(event.data["name"] for event in updated) == ["a", "b"]


async def test_delete_one_records_a_delete(users, events):
    user = await users.create(name="John")
    await users.delete_one(id=user.id)

    event = (await stored(events))[-1]
    assert event.type == "user.deleted"
    assert event.data["name"] == "John"


@pytest.mark.parametrize("method", ["remove", "delete_many"])
async def test_bare_deletes_record_events(users, events, method):
    await users.create_many([{"name": "a"}, {"name": "b"}])

    await getattr(users, method)(OutboxUser.name == "a")

    deleted = [e for e in await stored(events) if e.type == "user.deleted"]
    assert [event.data["name"] for event in deleted] == ["a"]
    assert await users.count() == 1


async def test_bulk_create_records_events_without_returning_results(users, events):
    result = await users.bulk_create([{"name": "a"}, {"name": "b"}])

    assert result is None
    created = [e for e in await stored(events) if e.type == "user.created"]
    assert sorted(event.data["name"] for event in created) == ["a", "b"]


async def test_bulk_create_returning_results_still_returns_rows(users, events):
    rows = await users.bulk_create([{"name": "a"}], return_results=True)

    assert [row.name for row in rows] == ["a"]
    assert len(await stored(events)) == 1


async def test_bulk_create_or_update_records_events(users, events):
    user = await users.create(name="a")
    result = await users.bulk_create_or_update([{"id": user.id, "name": "b"}])

    assert [row.name for row in result.scalars()] == ["b"]
    assert [event.type for event in await stored(events)] == [
        "user.created",
        "user.updated",
    ]


async def test_bulk_update_reads_the_rows_back(users, events):
    created = await users.create_many([{"name": "a"}, {"name": "b"}])
    await users.bulk_update(
        [{"id": row.id, "password": HASHED} for row in created],
    )

    updated = [e for e in await stored(events) if e.type == "user.updated"]
    assert sorted(event.data["name"] for event in updated) == ["a", "b"]


async def test_get_or_create_records_only_the_created_row(tags, events):
    first = await tags.get_or_create(label="x")
    again = await tags.get_or_create(label="x")

    assert again.id == first.id
    assert [event.type for event in await stored(events)] == ["test_outbox_tag.created"]


async def test_undeclared_operations_are_not_recorded(notes, events):
    note = await notes.create(body="hello")
    await notes.delete_one(id=note.id)

    assert [event.type for event in await stored(events)] == [
        "test_outbox_note.created"
    ]


async def test_repository_can_narrow_the_payload(events):
    await NarrowedUserRepository().create(name="John", password=HASHED)

    (event,) = await stored(events)
    assert event.data == {"name": "John"}


async def test_events_roll_back_with_the_row(users, events):
    class Failure(RuntimeError):
        pass

    @atomic
    async def write(repository) -> None:
        await repository.create(name="John")
        raise Failure

    with pytest.raises(Failure):
        await write(users)

    assert await users.count() == 0
    assert await events.count() == 0


# --- extra attributes ---


async def test_attributes_come_from_the_row_and_the_context(tenant_users, events):
    tenant = uuid4()
    traceparent.set("00-trace-span-01")

    await tenant_users.create(name="John", tenant_id=tenant)

    (event,) = await stored(events)
    assert event.attributes == {
        "tenant_id": str(tenant),
        "traceparent": "00-trace-span-01",
    }


async def test_a_promoted_column_leaves_the_payload(tenant_users, events):
    await tenant_users.create(name="John", tenant_id=uuid4())

    (event,) = await stored(events)
    assert "tenant_id" not in event.data
    assert event.data["name"] == "John"


async def test_events_carry_no_attributes_without_configuration(users, events):
    await users.create(name="John")

    (event,) = await stored(events)
    assert event.attributes is None


@pytest.mark.parametrize(
    ("operation", "event_type"),
    [("create", "user.created"), ("update_one", "user.updated")],
)
async def test_every_operation_carries_the_attributes(
    tenant_users, events, operation, event_type
):
    tenant = uuid4()
    user = await tenant_users.create(name="John", tenant_id=tenant)
    if operation == "update_one":
        await tenant_users.update_one({"name": "Jane"}, id=user.id)

    event = (await stored(events))[-1]
    assert event.type == event_type
    assert event.attributes["tenant_id"] == str(tenant)


async def test_deletes_carry_the_attributes(tenant_users, events):
    tenant = uuid4()
    user = await tenant_users.create(name="John", tenant_id=tenant)

    await tenant_users.delete_one(id=user.id)

    event = (await stored(events))[-1]
    assert event.type == "user.deleted"
    assert event.attributes["tenant_id"] == str(tenant)


async def test_bulk_writes_carry_the_attributes_of_each_row(tenant_users, events):
    tenants = [uuid4(), uuid4()]

    await tenant_users.bulk_create(
        [{"name": "a", "tenant_id": tenants[0]}, {"name": "b", "tenant_id": tenants[1]}]
    )

    recorded = {event.data["name"]: event.attributes for event in await stored(events)}
    assert recorded["a"]["tenant_id"] == str(tenants[0])
    assert recorded["b"]["tenant_id"] == str(tenants[1])


# --- event repository ---


async def test_claim_pending_leases_and_counts_attempts(users, events):
    await users.create(name="John")
    now = utc_now()

    claimed = await events.claim_pending(now, lease=timedelta(minutes=5))

    assert len(claimed) == 1
    assert claimed[0].attempts == 1
    assert claimed[0].available_at == now + timedelta(minutes=5)
    assert await events.claim_pending(now, lease=timedelta(minutes=5)) == []


async def test_claim_pending_skips_exhausted_events(users, events):
    await users.create(name="John")
    now = utc_now()

    await events.claim_pending(now, lease=timedelta(0), max_attempts=1)

    assert await events.claim_pending(now, lease=timedelta(0), max_attempts=1) == []


async def test_claim_pending_returns_events_in_write_order(users, events):
    for name in ("a", "b", "c"):
        await users.create(name=name)

    claimed = await events.claim_pending(utc_now(), lease=timedelta(minutes=5))

    assert [event.data["name"] for event in claimed] == ["a", "b", "c"]


async def test_mark_published_clears_the_error(users, events):
    await users.create(name="John")
    (event,) = await stored(events)
    await events.mark_failed(event.id, "boom", utc_now())

    await events.mark_published([event.id], utc_now())

    (stored_event,) = await stored(events)
    assert stored_event.published_at is not None
    assert stored_event.last_error is None
    assert await events.pending_count() == 0


async def test_mark_published_without_ids_is_a_no_op(events):
    await events.mark_published([], utc_now())

    assert await events.count() == 0


async def test_purge_only_deletes_published_events(users, events):
    await users.create_many([{"name": "a"}, {"name": "b"}])
    first, second = await stored(events)
    cutoff = utc_now()
    await events.mark_published([first.id], cutoff - timedelta(days=8))
    await events.mark_published([second.id], cutoff)

    deleted = await events.purge(cutoff - timedelta(days=7))

    assert deleted == 1
    assert [event.id for event in await stored(events)] == [second.id]


# --- relay ---


@pytest.fixture
def published():
    return []


@pytest.fixture
def relay(published):
    async def publisher(event: OutboxEvent) -> None:
        published.append(event)

    return OutboxRelay(publisher, repository=EventRepository(), poll_interval=0.05)


async def test_dispatch_once_publishes_and_marks(users, events, relay, published):
    await users.create(name="John")

    assert await relay.dispatch_once() == 1

    assert [event.data["name"] for event in published] == ["John"]
    assert await events.pending_count() == 0
    assert await relay.dispatch_once() == 0


async def test_dispatch_once_preserves_order(users, relay, published):
    for name in ("a", "b", "c"):
        await users.create(name=name)

    await relay.dispatch_once()

    assert [event.data["name"] for event in published] == ["a", "b", "c"]


async def test_a_failing_publisher_stops_the_batch(users, events, published):
    for name in ("a", "b"):
        await users.create(name=name)

    async def publisher(event: OutboxEvent) -> None:
        published.append(event)
        msg = "broker down"
        raise RuntimeError(msg)

    relay = OutboxRelay(
        publisher,
        repository=EventRepository(),
        retry_backoff=1.0,
        max_retry_delay=1.0,
    )

    assert await relay.dispatch_once() == 0
    assert [event.data["name"] for event in published] == ["a"]
    failed = next(e for e in await stored(events) if e.last_error)
    assert failed.data["name"] == "a"
    assert failed.last_error == "RuntimeError: broker down"
    assert failed.published_at is None


async def test_retry_delay_grows_and_is_capped(relay):
    relay.retry_backoff = 2.0
    relay.max_retry_delay = 4.0

    delays = [relay.retry_delay(attempts).total_seconds() for attempts in (1, 2, 3, 9)]

    assert delays == [1.0, 2.0, 4.0, 4.0]


async def test_purge_uses_the_configured_retention(users, events, published):
    await users.create(name="John")
    (event,) = await stored(events)
    await events.mark_published([event.id], utc_now() - timedelta(days=8))

    async def publisher(_event: OutboxEvent) -> None:
        published.append(_event)

    relay = OutboxRelay(
        publisher, repository=EventRepository(), retention=timedelta(days=7)
    )

    assert await relay.purge() == 1


async def test_a_tick_leaves_published_events_alone(users, events, relay):
    await users.create(name="John")
    (event,) = await stored(events)
    await events.mark_published([event.id], utc_now() - timedelta(days=8))

    await relay._tick()

    assert await events.count() == 1


async def parked(events: EventRepository) -> None:
    """Wait until the relay has finished its batch and is back in its sleep.

    Leaving ``running()`` cancels the relay wherever it happens to be, and a
    cancel landing inside a statement terminates the one connection the
    in-memory SQLite pool hands out -- which the next test then finds closed.
    The publishers below stretch the poll interval on their way through, so
    once the batch is marked published the relay stays parked long enough for
    the cancel to be harmless.
    """
    for _ in range(200):
        if not await events.pending_count():
            return
        await anyio.sleep(0.01)
    pytest.fail("the relay never drained the table")


async def test_run_publishes_in_the_background(users, events, published):
    await users.create(name="John")
    arrived = anyio.Event()

    async def publisher(event: OutboxEvent) -> None:
        published.append(event)
        relay.poll_interval = 5.0
        arrived.set()

    relay = OutboxRelay(publisher, repository=EventRepository(), poll_interval=0.05)
    async with relay.running():
        with anyio.fail_after(2):
            await arrived.wait()
        await parked(events)

    assert [event.data["name"] for event in published] == ["John"]


async def test_run_survives_a_failing_poll(
    users, events, published, monkeypatch, caplog
):
    await users.create(name="John")
    arrived = anyio.Event()
    calls = 0

    async def publisher(event: OutboxEvent) -> None:
        published.append(event)
        relay.poll_interval = 5.0
        arrived.set()

    relay = OutboxRelay(publisher, repository=EventRepository(), poll_interval=0.05)
    original = relay.repository.claim_pending

    async def flaky(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            msg = "database gone"
            raise RuntimeError(msg)
        return await original(*args, **kwargs)

    monkeypatch.setattr(relay.repository, "claim_pending", flaky)

    with caplog.at_level("ERROR", logger="sqlargon.outbox.relay"):
        async with relay.running():
            with anyio.fail_after(2):
                await arrived.wait()
            await parked(events)

    assert "Dispatching outbox events failed" in caplog.text
    assert [event.data["name"] for event in published] == ["John"]
