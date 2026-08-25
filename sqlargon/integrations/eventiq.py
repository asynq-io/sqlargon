from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, overload

from eventiq import CloudEvent

if TYPE_CHECKING:
    from eventiq import Service

    from sqlargon.outbox import OutboxEvent, Publisher

CloudEventT = TypeVar("CloudEventT", bound="CloudEvent[Any]")


@overload
def to_cloud_event(
    event: OutboxEvent, *, source: str | None = ...
) -> CloudEvent[Any]: ...


@overload
def to_cloud_event(
    event: OutboxEvent, *, event_class: type[CloudEventT], source: str | None = ...
) -> CloudEventT: ...


def to_cloud_event(
    event: OutboxEvent,
    *,
    event_class: type[CloudEvent[Any]] = CloudEvent,
    source: str | None = None,
) -> Any:
    """Build the CloudEvent an outbox row stands for.

    ``event_class`` is the class the message is validated against, so a
    service with a base of its own -- one carrying a ``tenant_id``, say --
    gets back that class rather than the plain
    :class:`~eventiq.CloudEvent`. The row's ``attributes`` are passed as
    top-level attributes of it, and the core CloudEvents ones always come
    from the row's own columns::

        message = to_cloud_event(event, event_class=TenantEvent)
    """
    attributes: dict[str, Any] = dict(event.attributes or {})
    attributes.update(
        id=event.id,
        time=event.created_at,
        subject=event.topic,
        type=event.type,
        source=event.source or source,
    )
    headers = {name: str(value) for name, value in (event.headers or {}).items()}
    return event_class.new(event.data, headers=headers or None, **attributes)


def eventiq_publisher(
    service: Service,
    *,
    event_class: type[CloudEvent[Any]] = CloudEvent,
    source: str | None = None,
) -> Publisher:
    """Build a relay publisher that hands outbox rows to an eventiq service.

    Every claimed event is built as ``event_class``, the base class the
    service publishes, and ``source`` is the one events carrying none of
    their own are given::

        relay = OutboxRelay(eventiq_publisher(service, event_class=TenantEvent))

        async with relay.running():
            ...
    """

    async def publish(event: OutboxEvent) -> None:
        message = to_cloud_event(event, event_class=event_class, source=source)
        await service.publish(message)

    return publish


__all__ = ["eventiq_publisher", "to_cloud_event"]
