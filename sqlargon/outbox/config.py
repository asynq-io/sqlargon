from collections.abc import Callable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

__all__ = [
    "ALL_OPERATIONS",
    "AttributeSource",
    "Operation",
    "OutboxConfig",
    "format_topic",
]

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


def format_topic(topic: str, row: Any) -> str:
    if "{" not in topic:
        return topic
    return topic.format(**vars(row))


@dataclass(frozen=True, slots=True)
class OutboxConfig:
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
