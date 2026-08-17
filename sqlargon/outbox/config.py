from collections.abc import Callable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

__all__ = ["ALL_OPERATIONS", "AttributeSource", "Operation", "OutboxConfig"]

# Where an extra CloudEvent attribute comes from: a name of an attribute of
# the written row, or a callable handed that row -- how a ContextVar is read
AttributeSource = str | Callable[[Any], Any]

CORE_ATTRIBUTES = frozenset(
    {
        "data",
        "datacontenttype",
        "dataschema",
        "id",
        "source",
        "specversion",
        "subject",
        "time",
        "topic",
        "type",
    }
)


class Operation(str, Enum):
    """The kind of write an outbox event was produced by."""

    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"


ALL_OPERATIONS = frozenset(Operation)


@dataclass(frozen=True, slots=True)
class OutboxConfig:
    """How the writes of one repository are turned into events.

    ``topic`` and ``type_prefix`` both default to the model's table name,
    giving events of type ``user.created`` on the ``user`` topic. ``exclude``
    keeps columns out of the payload -- a password hash has no business
    leaving the database -- while ``include`` states the payload columns
    outright and wins over ``exclude``.

    ``attributes`` names the values that ride next to ``type`` and ``source``
    rather than inside the payload, for a schema whose CloudEvent carries a
    tenant or a trace of its own::

        OutboxConfig(
            exclude={"tenant_id"},
            attributes={
                "tenant_id": "tenant_id",
                "traceparent": lambda _: trace_id.get(),
            },
        )

    They are read when the write happens, since the relay publishes long
    after the context the write ran in is gone.
    """

    topic: str | None = None
    type_prefix: str | None = None
    source: str | None = None
    exclude: AbstractSet[str] = frozenset()
    include: AbstractSet[str] | None = None
    operations: AbstractSet[Operation] = ALL_OPERATIONS
    attributes: Mapping[str, AttributeSource] = MappingProxyType({})

    def __post_init__(self) -> None:
        shadowed = CORE_ATTRIBUTES.intersection(self.attributes)
        if shadowed:
            msg = (
                f"{', '.join(sorted(shadowed))} name CloudEvents core attributes, "
                f"which every event carries of its own"
            )
            raise ValueError(msg)

    def includes_column(self, name: str) -> bool:
        """Whether the column is copied into the payload."""
        if self.include is not None:
            return name in self.include
        return name not in self.exclude
