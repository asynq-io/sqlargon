from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel
from sqlalchemy import TypeDecorator

from .json import JSON

P = TypeVar("P", bound=BaseModel)


class Pydantic(TypeDecorator, Generic[P]):

    impl = JSON
    cache_ok = True

    def __init__(self, pydantic_type: type[P], sa_column_type=None) -> None:
        super().__init__()
        self._pydantic_type = pydantic_type
        if sa_column_type is not None:
            self.impl = sa_column_type

    def process_bind_param(self, value, dialect) -> dict[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, self._pydantic_type):
            return value.dict()
        return self._pydantic_type.parse_obj(value).dict()

    def process_result_value(self, value, dialect) -> P | None:
        if value:
            return self._pydantic_type.parse_obj(value)
        return None
