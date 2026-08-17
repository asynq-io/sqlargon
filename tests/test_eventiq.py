"""Tests for the eventiq integration layer."""

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from eventiq import CloudEvent
from pydantic import ValidationError

from sqlargon import Base, Database
from sqlargon.integrations.eventiq import eventiq_publisher, to_cloud_event
from sqlargon.mixins import CreatedUpdatedMixin, UUIDModelMixin
from sqlargon.outbox import (
    OutboxConfig,
    OutboxEvent,
    OutboxEventRepository,
    OutboxRelay,
    OutboxRepository,
)
from sqlargon.types import GUID
from sqlargon.utils import utc_now

# Models defined at module level to avoid re-registration with --count=3


class PublishedOrder(UUIDModelMixin, CreatedUpdatedMixin, Base):
    __tablename__ = "test_eventiq_order"

    reference = sa.Column(sa.Unicode(255), nullable=True)
    tenant_id = sa.Column(GUID(), nullable=True)


class OrderRepository(OutboxRepository[PublishedOrder]):
    outbox = OutboxConfig(
        topic="orders",
        type_prefix="order",
        exclude={"tenant_id"},
        attributes={"tenant_id": "tenant_id"},
    )


class TenantEvent(CloudEvent[Any]):
    """The base class a service of its own would publish."""

    tenant_id: UUID


class FakeService:
    """Stands in for ``eventiq.Service``, which needs a broker to build."""

    def __init__(self) -> None:
        self.published: list[CloudEvent] = []

    async def publish(self, message: CloudEvent) -> None:
        self.published.append(message)


@pytest.fixture(autouse=True)
async def tables(db: Database):
    created = (PublishedOrder.__table__, OutboxEvent.__table__)
    async with db.engine.begin() as conn:
        for table in created:
            await conn.run_sync(table.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        for table in reversed(created):
            await conn.run_sync(table.drop, checkfirst=True)


@pytest.fixture
def orders():
    return OrderRepository()


@pytest.fixture
def events():
    return OutboxEventRepository()


def build_event(**overrides) -> OutboxEvent:
    values = {
        "id": uuid4(),
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "available_at": utc_now(),
        "topic": "orders",
        "type": "order.created",
        "source": None,
        "data": {"reference": "abc"},
        "attributes": None,
        "headers": None,
        "attempts": 0,
    }
    return OutboxEvent(**{**values, **overrides})


def test_to_cloud_event_maps_every_attribute():
    event = build_event(source="orders-service")

    message = to_cloud_event(event)

    assert message.id == event.id
    assert message.time == event.created_at
    assert message.topic == "orders"
    assert message.type == "order.created"
    assert message.source == "orders-service"
    assert message.data == {"reference": "abc"}
    assert message.specversion == "1.0"


def test_topic_serialises_as_the_cloud_events_subject():
    message = to_cloud_event(build_event())

    assert message.model_dump(by_alias=True)["subject"] == "orders"


def test_the_row_source_wins_over_the_fallback():
    message = to_cloud_event(build_event(source="row"), source="fallback")

    assert message.source == "row"


def test_the_fallback_source_is_used_when_the_row_has_none():
    message = to_cloud_event(build_event(source=None), source="fallback")

    assert message.source == "fallback"


def test_stored_attributes_become_top_level_attributes():
    tenant = uuid4()
    event = build_event(attributes={"tenant_id": str(tenant)})

    message = to_cloud_event(event, event_class=TenantEvent)

    assert isinstance(message, TenantEvent)
    assert message.tenant_id == tenant
    assert message.model_dump()["tenant_id"] == tenant


def test_the_base_cloud_event_keeps_unknown_attributes():
    message = to_cloud_event(build_event(attributes={"tenant_id": "acme"}))

    assert message.model_dump()["tenant_id"] == "acme"


def test_an_event_class_missing_an_attribute_is_rejected():
    with pytest.raises(ValidationError):
        to_cloud_event(build_event(), event_class=TenantEvent)


def test_stored_attributes_cannot_override_the_core_ones():
    event = build_event(attributes={"id": str(uuid4()), "type": "spoofed"})

    message = to_cloud_event(event)

    assert message.id == event.id
    assert message.type == "order.created"


def test_headers_are_handed_over_as_strings():
    message = to_cloud_event(build_event(headers={"retries": 2}))

    assert message.headers == {"retries": "2"}


async def test_the_publisher_hands_recorded_events_to_the_service(orders, events):
    service = FakeService()
    relay = OutboxRelay(
        eventiq_publisher(service, source="orders-service"),
        repository=OutboxEventRepository(),
    )
    await orders.create(reference="abc")

    assert await relay.dispatch_once() == 1

    (message,) = service.published
    assert message.topic == "orders"
    assert message.type == "order.created"
    assert message.source == "orders-service"
    assert message.data["reference"] == "abc"
    assert await events.pending_count() == 0


async def test_the_publisher_builds_the_configured_event_class(orders):
    service = FakeService()
    relay = OutboxRelay(
        eventiq_publisher(service, event_class=TenantEvent, source="orders-service"),
        repository=OutboxEventRepository(),
    )
    tenant = uuid4()
    await orders.create(reference="abc", tenant_id=tenant)

    assert await relay.dispatch_once() == 1

    (message,) = service.published
    assert isinstance(message, TenantEvent)
    assert message.tenant_id == tenant
    assert message.data["reference"] == "abc"
    assert "tenant_id" not in message.data


async def test_a_service_failure_leaves_the_event_pending(orders, events):
    class BrokenService(FakeService):
        async def publish(self, message: CloudEvent) -> None:
            msg = "broker down"
            raise RuntimeError(msg)

    relay = OutboxRelay(
        eventiq_publisher(BrokenService()),
        repository=OutboxEventRepository(),
        retry_backoff=1.0,
        max_retry_delay=1.0,
    )
    await orders.create(reference="abc")

    assert await relay.dispatch_once() == 0

    assert await events.pending_count() == 1
    (event,) = await events.select().all()
    assert event.last_error == "RuntimeError: broker down"
    assert event.available_at <= utc_now() + timedelta(seconds=1)
