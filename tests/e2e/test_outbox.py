"""The transactional outbox against a real server.

Capture runs on every backend: the repository reads its written rows back
through the same RETURNING fallback the rest of the library uses, so MySQL and
MariaDB -- which have no RETURNING at all -- take the select-write-refetch path
instead. See :mod:`tests.e2e.test_capabilities`.
"""

from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import anyio
import pytest

from sqlargon import Database
from sqlargon.outbox import OutboxEvent, OutboxEventRepository, OutboxRelay
from sqlargon.utils import utc_now

from .models import OutboxUser, OutboxUserRepository

if TYPE_CHECKING:
    from collections.abc import Sequence

HASHED = "not-a-real-secret"


class Failure(RuntimeError):
    pass


async def create_then_fail(db: Database, users: OutboxUserRepository) -> None:
    async with db.session_context():
        await users.create(name="John")
        raise Failure


@pytest.fixture
def events() -> OutboxEventRepository:
    return OutboxEventRepository()


# --- capture ---


async def test_create_records_an_event(
    outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    user = await outbox_users.create(name="John", password=HASHED)
    assert user is not None

    (event,) = await events.select().all()
    assert event.topic == "users"
    assert event.type == "user.created"
    assert event.source == "e2e"
    assert event.data["name"] == "John"
    assert event.data["id"] == str(user.id)
    assert "password" not in event.data
    assert event.published_at is None


async def test_update_and_delete_are_recorded(
    outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    user = await outbox_users.create(name="John")
    assert user is not None
    await outbox_users.update_one({"name": "Jane"}, id=user.id)
    await outbox_users.remove(OutboxUser.id == user.id)

    types = [
        event.type
        for event in await events.select().order_by(OutboxEvent.created_at).all()
    ]
    assert types == ["user.created", "user.updated", "user.deleted"]


async def test_an_upsert_records_what_each_row_turned_out_to_be(
    outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    """``is_new`` tells the inserted rows from the updated ones on every backend."""
    existing = await outbox_users.create(name="John")
    assert existing is not None

    # every column of the default conflict set is named, so the statement is
    # portable -- see test_capabilities.test_upsert_of_a_partial_row
    await outbox_users.bulk_create_or_update(
        [
            {"id": existing.id, "name": "Jane", "password": HASHED, "tenant_id": None},
            {"id": uuid4(), "name": "Jack", "password": HASHED, "tenant_id": None},
        ]
    )

    recorded = {
        event.data["name"]: event.type
        for event in await events.select().order_by(OutboxEvent.created_at).all()
    }
    assert recorded == {
        "John": "user.created",
        "Jane": "user.updated",
        "Jack": "user.created",
    }


async def test_an_upsert_leaves_the_creation_time_alone(
    outbox_users: OutboxUserRepository,
):
    user = await outbox_users.create(name="John")
    assert user is not None

    updated = await outbox_users.create_or_update(
        id=user.id, name="Jane", password=HASHED, tenant_id=None
    )

    assert updated.created_at == user.created_at
    assert updated.updated_at > updated.created_at


async def test_bulk_writes_record_one_event_per_row(
    outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    await outbox_users.bulk_create([{"name": "a"}, {"name": "b"}])

    created = await events.select().all()
    assert sorted(event.data["name"] for event in created) == ["a", "b"]


async def test_the_event_rolls_back_with_the_row(
    db: Database, outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    with pytest.raises(Failure):
        await create_then_fail(db, outbox_users)

    assert await outbox_users.count() == 0
    assert await events.count() == 0


async def test_the_payload_survives_a_round_trip(
    outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    """The JSON column and the engine serializer agree on the row's types."""
    user = await outbox_users.create(name="John")
    assert user is not None

    (event,) = await events.select().all()
    assert event.data["id"] == str(user.id)
    assert event.data["created_at"].startswith(str(user.created_at.year))


async def test_the_attributes_survive_a_round_trip(
    outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    tenant = uuid4()
    user = await outbox_users.create(name="John", tenant_id=tenant)
    assert user is not None

    (event,) = await events.select().all()
    assert event.attributes == {"tenant_id": str(tenant)}
    assert "tenant_id" not in event.data


# --- relay ---


async def test_the_relay_publishes_and_marks(
    outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    published: list[OutboxEvent] = []

    async def publisher(event: OutboxEvent) -> None:
        published.append(event)

    relay = OutboxRelay(publisher)
    await outbox_users.create(name="John")

    assert await relay.dispatch_once() == 1
    assert [event.data["name"] for event in published] == ["John"]
    assert await events.pending_count() == 0


async def test_the_relay_purges_published_events(
    outbox_users: OutboxUserRepository, events: OutboxEventRepository
):
    async def publisher(_event: OutboxEvent) -> None: ...

    relay = OutboxRelay(publisher, retention=timedelta(days=7))
    await outbox_users.create(name="John")
    (event,) = await events.select().all()
    await events.mark_published([event.id], utc_now() - timedelta(days=8))

    assert await relay.purge() == 1
    assert await events.count() == 0


@pytest.mark.usefixtures("needs_skip_locked")
async def test_concurrent_relays_never_claim_the_same_event(
    database_url: str, outbox_users: OutboxUserRepository
):
    await outbox_users.create(name="John")
    claimed: list[Sequence[OutboxEvent]] = []

    async def claim(database: Database) -> None:
        repository = OutboxEventRepository().using(db=database)
        claimed.append(
            await repository.claim_pending(utc_now(), lease=timedelta(minutes=5))
        )

    first, second = Database(database_url), Database(database_url)
    try:
        async with anyio.create_task_group() as group:
            group.start_soon(claim, first)
            group.start_soon(claim, second)
    finally:
        await first.dispose()
        await second.dispose()

    assert sorted(len(batch) for batch in claimed) == [0, 1]


# The background poll loop is dialect independent and covered by the unit
# suite; cancelling it here would abort whichever statement the driver has in
# flight and hand the next test a poisoned connection.
