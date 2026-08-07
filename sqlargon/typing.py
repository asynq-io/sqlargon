from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypedDict

SingleValue = Mapping[str, Any]
MultipleValues = Sequence[Mapping[str, Any]]
Values = SingleValue | MultipleValues

Params = Mapping[str, Any] | Sequence[Mapping[str, Any]]


class OnConflictOptions(TypedDict, total=False):
    index_elements: set[str]
    set_: set[str] | Mapping[str, Any]
    constraint: str
    index_where: Any
    where: Any
    exclude_set: set[str]


@dataclass(slots=True, frozen=True)
class OnConflict:
    do: Literal["ignore", "update"]
    options: OnConflictOptions


class WithForUpdate(TypedDict, total=False):
    nowait: bool
    read: bool
    of: Any
    skip_locked: bool
    key_share: bool
