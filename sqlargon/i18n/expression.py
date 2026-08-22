from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.compiler import compiles

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.sql.compiler import SQLCompiler
    from sqlalchemy.sql.elements import BindParameter, ColumnElement

_get_locale: Callable[[], str] | None = None


def set_locale_getter(fn: Callable[[], str]) -> None:
    """Register the callable that returns the active locale per request.

    The callable is invoked at SQL execution time via a late-binding bind
    parameter, so a single statement template is cached and reused across
    requests of any locale.
    """
    global _get_locale  # noqa: PLW0603
    _get_locale = fn


def get_locale() -> str:
    """Return the active locale for the current request.

    Falls through to the callable registered with :func:`set_locale_getter`
    at startup.
    """
    if _get_locale is None:
        msg = (
            "No locale getter has been configured. "
            "Call sqlargon.i18n.set_locale_getter() at startup."
        )
        raise RuntimeError(msg)
    return _get_locale()


def current_locale() -> BindParameter[str]:
    """Bind parameter resolving to the active locale on every execution.

    Late binding keeps cached statements -- relationship join conditions in
    particular, which are built once when mappers are configured -- aware of
    the locale of the request being served.
    """
    return sa.bindparam(
        "current_locale", callable_=get_locale, type_=sa.String, unique=True
    )


def _locale_path() -> BindParameter[str]:
    return sa.bindparam(
        "locale_path", callable_=_current_locale_path, type_=sa.String, unique=True
    )


def _current_locale_path() -> str:
    return f'$."{get_locale()}"'


class translated_value(sa.FunctionElement[str]):
    """Text stored under the active locale key of a JSON translation column."""

    name = "translated_value"
    type = sa.String()
    inherit_cache = True


def _operand(element: translated_value) -> ColumnElement[Any]:
    """Read the wrapped column off the element itself.

    Clone and adapt machinery -- `ClauseAdapter`, `with_loader_criteria`,
    `with_polymorphic` -- rewrites only the traversed ``clauses``, so anything
    cached on the instance would still point at the pre-adaption column.
    """
    return next(iter(element.clauses))


@compiles(translated_value, "postgresql")
def _compile_postgresql(
    element: translated_value, compiler: SQLCompiler, **kwargs: Any
) -> str:
    column = sa.type_coerce(_operand(element), postgresql.JSONB)
    return compiler.process(
        column.op("->>")(sa.cast(current_locale(), sa.Text)), **kwargs
    )


@compiles(translated_value)
def _compile_default(
    element: translated_value, compiler: SQLCompiler, **kwargs: Any
) -> str:
    return compiler.process(
        sa.func.json_extract(_operand(element), _locale_path()), **kwargs
    )
