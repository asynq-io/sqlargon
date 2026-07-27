from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, TypeAdapter
from sqlalchemy import Dialect, TypeDecorator

from .json import JSON

P = TypeVar("P", bound=BaseModel)
T = TypeVar("T")


class Pydantic(TypeDecorator, Generic[P]):
    impl = JSON
    cache_ok = True

    def __init__(self, pydantic_type: type[P], sa_column_type: Any = None) -> None:
        super().__init__()
        self._pydantic_type = pydantic_type
        if sa_column_type is not None:
            self.impl = sa_column_type

    def process_bind_param(self, value: Any, dialect: Dialect) -> dict[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, self._pydantic_type):
            return value.model_dump()
        return self._pydantic_type.model_validate(value).model_dump()

    def process_result_value(self, value: Any, dialect: Dialect) -> P | None:
        if value is None:
            return None
        return self._pydantic_type.model_validate(value)


class ValidatedType(TypeDecorator, Generic[T]):
    impl = JSON
    cache_ok = True

    def __init__(
        self, type_: T, *, validate: bool = True, sa_column_type: Any = None
    ) -> None:
        super().__init__()
        self._type_adapter: TypeAdapter[T] = TypeAdapter(type_)
        self._validate = validate
        if sa_column_type is not None:
            self.impl = sa_column_type

    def process_bind_param(self, value: Any, dialect: Dialect) -> Any:
        if value is None:
            return None
        if self._validate:
            value = self._type_adapter.validate_python(value)
        return self._type_adapter.dump_python(value, warnings=False)

    def process_result_value(self, value: Any, dialect: Dialect) -> T | None:
        if value is None:
            return None
        return self._type_adapter.validate_python(value)
