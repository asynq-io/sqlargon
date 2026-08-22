from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from pydantic_core import core_schema
from sqlalchemy import TypeDecorator

from sqlargon.types import JSON

from .expression import get_locale, translated_value

if TYPE_CHECKING:
    from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
    from pydantic.json_schema import JsonSchemaValue
    from sqlalchemy import ColumnElement, Dialect
    from sqlalchemy.sql.operators import OperatorType
    from typing_extensions import Self

LocaleMap = dict[str, str]

_get_fallback: Callable[[str | None], tuple[str, ...]] | None = None


def set_fallback_chain(fn: Callable[[str | None], tuple[str, ...]]) -> None:
    """Register the callable that builds the fallback chain for ``locale``.

    The callable receives an explicit locale or ``None`` (meaning "the
    active one") and is expected to return the ordered locales to try.
    """
    global _get_fallback  # noqa: PLW0603
    _get_fallback = fn


def fallback_chain(locale: str | None = None) -> tuple[str, ...]:
    """Return the locales to look up, best match first."""
    if _get_fallback is None:
        msg = (
            "No fallback chain has been configured. "
            "Call sqlargon.i18n.set_fallback_chain() at startup."
        )
        raise RuntimeError(msg)
    return _get_fallback(locale)


def select_current(data: Mapping[str, str], locale: str | None = None) -> str | None:
    """Pick the value for ``locale`` walking down its fallback chain."""
    for candidate in fallback_chain(locale):
        value = data.get(candidate)
        if value is not None:
            return value
    return next(iter(data.values()), None)


def _input_schema() -> core_schema.CoreSchema:
    """The shapes `Translation._validate` accepts: text, or ``{locale: text}``."""
    return core_schema.union_schema(
        [
            core_schema.str_schema(),
            core_schema.dict_schema(core_schema.str_schema(), core_schema.str_schema()),
        ]
    )


class Translation(str):  # noqa: SLOT000
    """Current locale text that also carries every other known translation."""

    _data: LocaleMap

    def __new__(cls, current: str, data: Mapping[str, str] | None = None) -> Self:
        translation = super().__new__(cls, current)
        translation._data = dict(data) if data is not None else {get_locale(): current}
        return translation

    @property
    def data(self) -> LocaleMap:
        """All known translations, keyed by locale."""
        return dict(self._data)

    def get(self, locale: str) -> str | None:
        """Return the text for ``locale``, or ``None`` when it is missing."""
        return self._data.get(locale)

    def update(self, value: str, locale: str | None = None) -> Translation:
        """Return a copy with ``value`` stored under ``locale``."""
        data = dict(self._data)
        data[locale or get_locale()] = value
        return Translation(select_current(data) or value, data)

    @classmethod
    def _validate(cls, value: Any) -> Translation:
        translation = as_translation(value)
        if translation is None:
            msg = "Input should be a string or a mapping of locales to strings"
            raise ValueError(msg)
        return translation

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls._validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                str, return_schema=core_schema.str_schema()
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        serialization = (
            schema.get("serialization") if handler.mode == "serialization" else None
        )
        return_schema = serialization.get("return_schema") if serialization else None
        return handler(return_schema or _input_schema())


def as_translation(value: Any, locale: str | None = None) -> Translation | None:
    """Normalize a raw attribute value into a `Translation`.

    Unusable input raises `ValueError`, the only error pydantic turns into a
    validation error -- a `TypeError` would leak out of validation as a 500.
    """
    if value is None:
        return None
    if isinstance(value, Translation):
        return value
    if isinstance(value, str):
        return Translation(value, {locale or get_locale(): value})
    if isinstance(value, Mapping):
        data = {str(key): _text(key, text) for key, text in value.items()}
        return Translation(select_current(data, locale) or "", data)
    msg = f"Cannot build a Translation from {type(value).__name__}"
    raise ValueError(msg)


def _text(key: Any, value: Any) -> str:
    """Reject non-string texts instead of stringifying them."""
    if not isinstance(value, str):
        msg = (
            f"Translation of locale {key!r} must be a string, "
            f"got {type(value).__name__}"
        )
        raise ValueError(msg)  # noqa: TRY004
    return value


def _locale_map(value: Any) -> LocaleMap | None:
    translation = as_translation(value)
    return None if translation is None else translation.data


class TranslatedString(TypeDecorator[Translation]):
    """JSON column holding ``{locale: text}`` and reading as a `Translation`.

    Every column operator is rewritten to act on the text of the locale active
    when the statement runs, so plain ``select(...).where(Model.field == value)``
    needs no join. The JSON methods inherited from `JSON.ComparatorFactory` --
    ``contains``, ``has_any_key``, ``has_all_keys``, ``json_value`` and
    indexing -- still address the whole locale map.
    """

    impl = JSON
    cache_ok = True

    class ComparatorFactory(JSON.ComparatorFactory):
        """Redirects every column operator to the active locale's text."""

        @property
        def current(self) -> ColumnElement[str]:
            return translated_value(self.expr)

        def operate(
            self, op: OperatorType, *other: Any, **kwargs: Any
        ) -> ColumnElement[Any]:
            return self.current.operate(op, *other, **kwargs)

        def reverse_operate(
            self, op: OperatorType, other: Any, **kwargs: Any
        ) -> ColumnElement[Any]:
            return self.current.reverse_operate(op, other, **kwargs)

        def asc(self) -> ColumnElement[str]:
            return self.current.asc()

        def desc(self) -> ColumnElement[str]:
            return self.current.desc()

    comparator_factory = ComparatorFactory  # pyright: ignore[reportAssignmentType, reportIncompatibleMethodOverride]

    def compare_values(self, x: Any, y: Any) -> bool:
        """Compare the whole locale maps, not just the active locale text."""
        return _locale_map(x) == _locale_map(y)

    def process_bind_param(
        self,
        value: Any,
        dialect: Dialect,  # noqa: ARG002
    ) -> LocaleMap | None:
        return _locale_map(value)

    def process_result_value(
        self,
        value: Any,
        dialect: Dialect,  # noqa: ARG002
    ) -> Translation | None:
        if value is None:
            return None
        data = {str(key): text for key, text in value.items() if isinstance(text, str)}
        return Translation(select_current(data) or "", data)
