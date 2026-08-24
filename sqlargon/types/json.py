from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy import Dialect, FunctionElement, TypeDecorator
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import coercions, roles
from sqlalchemy.sql.elements import Grouping

from sqlargon.utils import json_dumps

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy.sql.elements import ColumnElement
    from sqlalchemy.types import TypeEngine

__all__ = [
    "JSON",
    "json_array_append",
    "json_array_length",
    "json_contains",
    "json_get",
    "json_has_all_keys",
    "json_has_any_key",
    "json_has_key",
    "json_insert_key",
    "json_keys",
    "json_remove_key",
    "json_replace_key",
    "json_set_key",
    "json_update",
    "json_value",
]


def _json_path(key: str) -> str:
    """``key`` as a top level JSON path, quotes and backslashes escaped."""
    escaped = key.replace("\\", "\\\\").replace('"', '\\"')
    return f'$."{escaped}"'


def _operands(element: FunctionElement[Any]) -> tuple[ColumnElement[Any], ...]:
    """The element's operands, read off its own clause list.

    Two reasons never to reach for an attribute cached on the instance.
    Clone and adapt machinery -- ``ClauseAdapter``, ``with_loader_criteria``,
    ``with_polymorphic`` -- rewrites only the traversed ``clauses``, so a
    cached operand would still point at the pre-adaption column. And keys,
    paths and values are bind parameters in this list rather than literals
    baked in at compile time, which is what lets one cached statement serve
    every key: a bind created inside a ``@compiles`` hook is invisible to the
    statement cache, so the first key compiled would be reused for the rest.
    """
    return tuple(element.clauses)


def _text(value: str) -> ColumnElement[str]:
    return sa.literal(value, sa.String())


def _json(value: Any) -> ColumnElement[Any]:
    """``value`` as a JSON operand: an existing expression, or a bind."""
    return coercions.expect(roles.ExpressionElementRole, value, type_=JSON())


def _sqlite_json(value: ColumnElement[Any]) -> ColumnElement[Any]:
    """Re-parse a bound JSON string into a JSON value.

    Without it ``json_set`` would store the serialized text as a JSON
    *string* rather than as the document it represents.
    """
    return sa.func.json(value)


def _mysql_json(value: ColumnElement[Any]) -> ColumnElement[Any]:
    """As :func:`_sqlite_json`, for the MySQL family.

    ``json_extract(:v, '$')`` rather than ``CAST(:v AS JSON)`` because
    MariaDB's ``JSON`` is a ``LONGTEXT`` alias whose cast support differs
    from MySQL's, while both spell the whole-document extract this way.
    """
    return sa.func.json_extract(value, _text("$"))


def _pg_json(value: ColumnElement[Any]) -> ColumnElement[Any]:
    return sa.cast(value, postgresql.JSONB)


def _pg_group(expression: ColumnElement[Any]) -> ColumnElement[Any]:
    """Parenthesize a PostgreSQL JSON operator expression.

    These elements compile to infix operators, but to an enclosing compiler
    they are opaque functions with no precedence to reason about. Nesting a
    removal around a merge would otherwise emit ``a || b - c``, which
    PostgreSQL reads as ``a || (b - c)`` -- binary ``-`` binds tighter than
    ``||`` -- and so would drop the key from the patch instead of from the
    merged document.
    """
    return Grouping(expression)


class json_contains(FunctionElement):
    """
    Platform independent json_contains operator, tests if the
    `left` expression contains the `right` expression.

    On postgres this is equivalent to the @> containment operator.
    https://www.postgresql.org/docs/current/functions-json.html
    """

    type = sa.Boolean()
    name = "json_contains"
    inherit_cache = False

    def __init__(self, left: Any, right: Any) -> None:
        self.left = left
        self.right = right
        super().__init__()


@compiles(json_contains, "postgresql")
def _json_contains_postgresql(
    element: json_contains, compiler: Any, **kwargs: Any
) -> str:
    return compiler.process(
        sa.type_coerce(element.left, postgresql.JSONB).contains(
            sa.type_coerce(element.right, postgresql.JSONB)
        ),
        **kwargs,
    )


def _json_contains_sqlite_fn(
    left: Any, right: Any, compiler: Any, **_kwargs: Any
) -> str:
    if isinstance(left, (list, dict, tuple, str)):
        left = json_dumps(left)

    if isinstance(right, (list, dict, tuple, str)):
        right = json_dumps(right)

    json_each_left = sa.func.json_each(left).alias("left")
    json_each_right = sa.func.json_each(right).alias("right")

    distinct_matches = (
        sa.select(sa.func.count(sa.distinct(sa.literal_column("left.value"))))
        .select_from(json_each_left)
        .join(
            json_each_right,
            sa.literal_column("left.value") == sa.literal_column("right.value"),
        )
        .scalar_subquery()
    )

    distinct_keys = (
        sa.select(sa.func.count(sa.distinct(sa.literal_column("right.value"))))
        .select_from(json_each_right)
        .scalar_subquery()
    )

    return compiler.process(distinct_matches >= distinct_keys)


@compiles(json_contains, "sqlite")
def _json_contains_sqlite(element: json_contains, compiler: Any, **kwargs: Any) -> str:
    return _json_contains_sqlite_fn(element.left, element.right, compiler, **kwargs)


@compiles(json_contains, "mysql")
@compiles(json_contains)
def _json_contains_mysql(element: json_contains, compiler: Any, **kwargs: Any) -> str:
    return compiler.process(
        sa.func.json_contains(
            sa.type_coerce(element.left, sa.JSON),
            sa.type_coerce(element.right, sa.JSON),
        ),
        **kwargs,
    )


class json_has_any_key(FunctionElement):
    """
    Platform independent json_has_any_key operator.

    On postgres this is equivalent to the ?| existence operator.
    https://www.postgresql.org/docs/current/functions-json.html
    """

    type: Any = sa.Boolean()
    name = "json_has_any_key"
    inherit_cache = False

    def __init__(self, json_expr: Any, values: list) -> None:
        self.json_expr = json_expr
        if not all(isinstance(v, str) for v in values):
            msg = "json_has_any_key values must be strings"
            raise ValueError(msg)
        self.values = values
        super().__init__()


@compiles(json_has_any_key, "postgresql")
@compiles(json_has_any_key)
def _json_has_any_key_postgresql(
    element: json_has_any_key, compiler: Any, **kwargs: Any
) -> str:
    values_array = postgresql.array(element.values)
    # if the array is empty, postgres requires a type annotation
    if not element.values:
        values_array = sa.cast(values_array, postgresql.ARRAY(sa.String))

    return compiler.process(
        sa.type_coerce(element.json_expr, postgresql.JSONB).has_any(values_array),
        **kwargs,
    )


@compiles(json_has_any_key, "sqlite")
def _json_has_any_key_sqlite(
    element: json_has_any_key, compiler: Any, **kwargs: Any
) -> str:
    json_each = sa.func.json_each(element.json_expr).alias("json_each")
    return compiler.process(
        sa.select(1)
        .select_from(json_each)
        .where(
            sa.literal_column("json_each.value").in_(
                sa.bindparam(key="json_each_values", value=element.values, unique=True)
            )
        )
        .exists(),
        **kwargs,
    )


@compiles(json_has_any_key, "mysql")
def _json_has_any_key_mysql(
    element: json_has_any_key, compiler: Any, **kwargs: Any
) -> str:
    if not element.values:
        return compiler.process(sa.false(), **kwargs)
    paths = [sa.literal(f"$.{key}") for key in element.values]
    return compiler.process(
        sa.func.json_contains_path(element.json_expr, sa.literal("one"), *paths),
        **kwargs,
    )


class json_has_all_keys(FunctionElement):
    """Platform independent json_has_all_keys operator.

    On postgres this is equivalent to the ?& existence operator.
    https://www.postgresql.org/docs/current/functions-json.html
    """

    type: Any = sa.Boolean()
    name = "json_has_all_keys"
    inherit_cache = False

    def __init__(self, json_expr: Any, values: list) -> None:
        self.json_expr = json_expr
        if isinstance(values, list) and not all(isinstance(v, str) for v in values):
            msg = (
                "json_has_all_key values must be strings if provided as a literal list"
            )
            raise ValueError(msg)
        self.values = values
        super().__init__()


@compiles(json_has_all_keys, "postgresql")
@compiles(json_has_all_keys)
def _json_has_all_keys_postgresql(
    element: json_has_all_keys, compiler: Any, **kwargs: Any
) -> str:
    values_array = postgresql.array(element.values)

    # if the array is empty, postgres requires a type annotation
    if not element.values:
        values_array = sa.cast(values_array, postgresql.ARRAY(sa.String))

    return compiler.process(
        sa.type_coerce(element.json_expr, postgresql.JSONB).has_all(values_array),
        **kwargs,
    )


@compiles(json_has_all_keys, "sqlite")
def _json_has_all_keys_sqlite(
    element: json_has_all_keys, compiler: Any, **kwargs: Any
) -> str:
    # "has all keys" is equivalent to "json contains"
    return _json_contains_sqlite_fn(
        left=element.json_expr,
        right=element.values,
        compiler=compiler,
        **kwargs,
    )


@compiles(json_has_all_keys, "mysql")
def _json_has_all_keys_mysql(
    element: json_has_all_keys, compiler: Any, **kwargs: Any
) -> str:
    if not element.values:
        return compiler.process(sa.true(), **kwargs)
    paths = [sa.literal(f"$.{key}") for key in element.values]
    return compiler.process(
        sa.func.json_contains_path(element.json_expr, sa.literal("all"), *paths),
        **kwargs,
    )


class json_value(FunctionElement[str]):
    """Portable ``->>`` operator: text value at a JSON object key."""

    name = "json_value"
    type = sa.String()
    inherit_cache = True

    def __init__(self, column: Any, key: str) -> None:
        self.column = column
        self.key = key
        super().__init__(column, _text(key), _text(_json_path(key)))


@compiles(json_value, "postgresql")
def _json_value_postgresql(element: json_value, compiler: Any, **kwargs: Any) -> str:
    column, key, _path = _operands(element)
    return compiler.process(
        sa.type_coerce(column, postgresql.JSONB).op("->>")(key), **kwargs
    )


@compiles(json_value)
def _json_value_default(element: json_value, compiler: Any, **kwargs: Any) -> str:
    column, _key, path = _operands(element)
    return compiler.process(sa.func.json_extract(column, path), **kwargs)


class JSON(TypeDecorator):
    """
    JSON type that returns SQLAlchemy's dialect-specific JSON types, where
    possible. Uses generic JSON otherwise.

    The "base" type is postgresql.JSONB to expose useful methods prior
    to SQL compilation
    """

    impl = postgresql.JSONB
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(postgresql.JSONB(none_as_null=True))
        if dialect.name == "sqlite":
            return dialect.type_descriptor(sqlite.JSON(none_as_null=True))
        return dialect.type_descriptor(sa.JSON(none_as_null=True))

    def literal_processor(self, dialect: Dialect) -> Callable[[Any], str]:
        """Render the value as an inline SQL string literal.

        Only reached under ``literal_binds``, so when a statement is being
        printed or logged rather than executed. SQLAlchemy's own JSON types
        ship no literal renderer, so without this a statement carrying a
        JSON bind cannot be compiled at all. The serialized text is quoted by
        the dialect's own string renderer rather than by hand, which is what
        gets the backslash escaping right on MySQL.
        """
        quote = sa.String().literal_processor(dialect)
        if quote is None:  # pragma: no cover - every dialect renders strings
            msg = f"{dialect.name} cannot render a string literal"
            raise NotImplementedError(msg)

        def process(value: Any) -> str:
            if value is None:
                return "NULL"
            return quote(json_dumps(value))

        return process

    class ComparatorFactory(sa.JSON.Comparator):
        """Type-specific SQL expression methods for a JSON column.

        Despite the name, this hook is not limited to comparisons -- the
        ``Comparator`` base already carries ``concat``, ``collate``,
        ``distinct`` and the arithmetic operators. The mutation methods below
        follow ``postgresql.JSONB.Comparator.delete_path`` and the HSTORE
        comparator's ``delete`` / ``slice`` / ``keys`` / ``vals``, which
        likewise answer with a rewritten container rather than a boolean.

        Nothing here mutates anything: each method returns an expression that
        is inert until it lands in an ``UPDATE``, at which point the document
        is rewritten by the server.
        """

        def contains(self, other: Any, **_kw: Any) -> json_contains:
            return json_contains(self, other)

        def has_any_key(self, other: Any) -> json_has_any_key:
            return json_has_any_key(self, other)

        def has_all_keys(self, other: Any) -> json_has_all_keys:
            return json_has_all_keys(self, other)

        def json_value(self, other: Any) -> json_value:
            return json_value(self, other)

        def has_key(self, other: str) -> json_has_key:
            return json_has_key(self, other)

        def get(self, other: str) -> json_get:
            return json_get(self, other)

        def keys(self) -> json_keys:
            return json_keys(self)

        def array_length(self) -> json_array_length:
            return json_array_length(self)

        def update(self, other: Mapping[str, Any]) -> json_update:
            return json_update(self, other)

        def set_key(self, key: str, value: Any) -> json_update:
            return json_set_key(self, key, value)

        def remove_key(self, *keys: str) -> json_remove_key:
            return json_remove_key(self, *keys)

        def insert_key(self, key: str, value: Any) -> json_insert_key:
            return json_insert_key(self, key, value)

        def replace_key(self, key: str, value: Any) -> json_replace_key:
            return json_replace_key(self, key, value)

        def array_append(self, value: Any) -> json_array_append:
            return json_array_append(self, value)

    comparator_factory = ComparatorFactory


# --- mutations -------------------------------------------------------------
#
# Every element below is typed ``JSON()``, so they nest freely:
# ``json_remove_key(json_update(col, {"a": 1}), "b")``. A SQL NULL column
# propagates -- ``jsonb_set`` and ``JSON_SET`` both return NULL for a NULL
# document and these do not paper over it -- and the ``JSON_SET`` family
# assumes the document is an object.


class json_update(FunctionElement[Any]):
    """Shallow merge of ``mapping`` into a JSON object.

    Top level keys of ``mapping`` replace their counterparts wholesale; a
    nested object is *not* merged recursively. Equivalent to the PostgreSQL
    ``||`` operator, which is what the multi-pair ``JSON_SET`` on the other
    backends reproduces -- deliberately not ``json_patch`` /
    ``JSON_MERGE_PATCH``, whose recursive semantics PostgreSQL cannot express
    without a recursive query.
    """

    name = "json_update"
    type = JSON()
    inherit_cache = True

    def __init__(self, column: Any, mapping: Mapping[str, Any]) -> None:
        if not all(isinstance(key, str) for key in mapping):
            msg = "json_update keys must be strings"
            raise ValueError(msg)
        pairs: list[ColumnElement[Any]] = []
        for key, value in mapping.items():
            pairs += [_text(_json_path(key)), _json(value)]
        # the whole mapping rides along as a single operand for the
        # PostgreSQL concat, the per-key pairs for the JSON_SET backends
        super().__init__(_json(column), _json(dict(mapping)), *pairs)


@compiles(json_update, "postgresql")
def _json_update_postgresql(element: json_update, compiler: Any, **kwargs: Any) -> str:
    column, mapping, *pairs = _operands(element)
    if not pairs:
        return compiler.process(column, **kwargs)
    merged = sa.type_coerce(column, postgresql.JSONB).op(
        "||", return_type=postgresql.JSONB
    )(_pg_json(mapping))
    return compiler.process(_pg_group(merged), **kwargs)


@compiles(json_update, "sqlite")
def _json_update_sqlite(element: json_update, compiler: Any, **kwargs: Any) -> str:
    column, _mapping, *pairs = _operands(element)
    if not pairs:
        return compiler.process(column, **kwargs)
    args: list[ColumnElement[Any]] = []
    for path, value in zip(pairs[::2], pairs[1::2], strict=True):
        args += [path, _sqlite_json(value)]
    return compiler.process(sa.func.json_set(column, *args), **kwargs)


@compiles(json_update, "mysql")
@compiles(json_update)
def _json_update_mysql(element: json_update, compiler: Any, **kwargs: Any) -> str:
    column, _mapping, *pairs = _operands(element)
    if not pairs:
        return compiler.process(column, **kwargs)
    args: list[ColumnElement[Any]] = []
    for path, value in zip(pairs[::2], pairs[1::2], strict=True):
        args += [path, _mysql_json(value)]
    return compiler.process(sa.func.json_set(column, *args), **kwargs)


def json_set_key(column: Any, key: str, value: Any) -> json_update:
    """Set one top level ``key``, replacing any value already there."""
    return json_update(column, {key: value})


class json_remove_key(FunctionElement[Any]):
    """Drop top level ``keys`` from a JSON object.

    On postgres this is the ``-`` operator over a ``text[]`` of keys.
    """

    name = "json_remove_key"
    type = JSON()
    inherit_cache = True

    def __init__(self, column: Any, *keys: str) -> None:
        if not all(isinstance(key, str) for key in keys):
            msg = "json_remove_key keys must be strings"
            raise ValueError(msg)
        super().__init__(
            _json(column),
            sa.literal(list(keys), postgresql.ARRAY(sa.Text)),
            *(_text(_json_path(key)) for key in keys),
        )


@compiles(json_remove_key, "postgresql")
def _json_remove_key_postgresql(
    element: json_remove_key, compiler: Any, **kwargs: Any
) -> str:
    column, keys, *paths = _operands(element)
    if not paths:
        return compiler.process(column, **kwargs)
    removed = sa.type_coerce(column, postgresql.JSONB).op(
        "-", return_type=postgresql.JSONB
    )(sa.cast(keys, postgresql.ARRAY(sa.Text)))
    return compiler.process(_pg_group(removed), **kwargs)


@compiles(json_remove_key, "sqlite")
@compiles(json_remove_key, "mysql")
@compiles(json_remove_key)
def _json_remove_key_default(
    element: json_remove_key, compiler: Any, **kwargs: Any
) -> str:
    column, _keys, *paths = _operands(element)
    # json_remove(col) with no path is a syntax error, and removing nothing
    # is the column itself
    if not paths:
        return compiler.process(column, **kwargs)
    return compiler.process(sa.func.json_remove(column, *paths), **kwargs)


class json_insert_key(FunctionElement[Any]):
    """Set ``key`` to ``value``, but only where the key is absent."""

    name = "json_insert_key"
    type = JSON()
    inherit_cache = True

    def __init__(self, column: Any, key: str, value: Any) -> None:
        super().__init__(
            _json(column), _json({key: value}), _text(_json_path(key)), _json(value)
        )


@compiles(json_insert_key, "postgresql")
def _json_insert_key_postgresql(
    element: json_insert_key, compiler: Any, **kwargs: Any
) -> str:
    column, mapping, _path, _value = _operands(element)
    # the new key on the left, so an existing one on the right wins
    inserted = _pg_json(mapping).op("||", return_type=postgresql.JSONB)(
        sa.type_coerce(column, postgresql.JSONB)
    )
    return compiler.process(_pg_group(inserted), **kwargs)


@compiles(json_insert_key, "sqlite")
def _json_insert_key_sqlite(
    element: json_insert_key, compiler: Any, **kwargs: Any
) -> str:
    column, _mapping, path, value = _operands(element)
    return compiler.process(
        sa.func.json_insert(column, path, _sqlite_json(value)), **kwargs
    )


@compiles(json_insert_key, "mysql")
@compiles(json_insert_key)
def _json_insert_key_mysql(
    element: json_insert_key, compiler: Any, **kwargs: Any
) -> str:
    column, _mapping, path, value = _operands(element)
    return compiler.process(
        sa.func.json_insert(column, path, _mysql_json(value)), **kwargs
    )


class json_replace_key(FunctionElement[Any]):
    """Set ``key`` to ``value``, but only where the key is already present."""

    name = "json_replace_key"
    type = JSON()
    inherit_cache = True

    def __init__(self, column: Any, key: str, value: Any) -> None:
        super().__init__(
            _json(column), _text(key), _text(_json_path(key)), _json(value)
        )


@compiles(json_replace_key, "postgresql")
def _json_replace_key_postgresql(
    element: json_replace_key, compiler: Any, **kwargs: Any
) -> str:
    column, key, _path, value = _operands(element)
    return compiler.process(
        sa.func.jsonb_set(
            sa.type_coerce(column, postgresql.JSONB),
            sa.cast(postgresql.array([key]), postgresql.ARRAY(sa.Text)),
            _pg_json(value),
            # create_missing => false is what makes this replace only
            sa.false(),
        ),
        **kwargs,
    )


@compiles(json_replace_key, "sqlite")
def _json_replace_key_sqlite(
    element: json_replace_key, compiler: Any, **kwargs: Any
) -> str:
    column, _key, path, value = _operands(element)
    return compiler.process(
        sa.func.json_replace(column, path, _sqlite_json(value)), **kwargs
    )


@compiles(json_replace_key, "mysql")
@compiles(json_replace_key)
def _json_replace_key_mysql(
    element: json_replace_key, compiler: Any, **kwargs: Any
) -> str:
    column, _key, path, value = _operands(element)
    return compiler.process(
        sa.func.json_replace(column, path, _mysql_json(value)), **kwargs
    )


class json_array_append(FunctionElement[Any]):
    """Append ``value`` as one element of a JSON array.

    A list ``value`` is appended as a single nested array, not concatenated,
    so the three backends agree.
    """

    name = "json_array_append"
    type = JSON()
    inherit_cache = True

    def __init__(self, column: Any, value: Any) -> None:
        super().__init__(_json(column), _json(value))


@compiles(json_array_append, "postgresql")
def _json_array_append_postgresql(
    element: json_array_append, compiler: Any, **kwargs: Any
) -> str:
    column, value = _operands(element)
    # build_array, not a bare concat: concatenating two arrays would merge
    # them instead of appending one element
    appended = sa.type_coerce(column, postgresql.JSONB).op(
        "||", return_type=postgresql.JSONB
    )(sa.func.jsonb_build_array(_pg_json(value)))
    return compiler.process(_pg_group(appended), **kwargs)


@compiles(json_array_append, "sqlite")
def _json_array_append_sqlite(
    element: json_array_append, compiler: Any, **kwargs: Any
) -> str:
    column, value = _operands(element)
    return compiler.process(
        sa.func.json_insert(column, _text("$[#]"), _sqlite_json(value)), **kwargs
    )


@compiles(json_array_append, "mysql")
@compiles(json_array_append)
def _json_array_append_mysql(
    element: json_array_append, compiler: Any, **kwargs: Any
) -> str:
    column, value = _operands(element)
    return compiler.process(
        sa.func.json_array_append(column, _text("$"), _mysql_json(value)), **kwargs
    )


# --- reads -----------------------------------------------------------------


class json_get(FunctionElement[Any]):
    """Portable ``->`` operator: the JSON value at a top level key.

    The JSON-typed counterpart of :class:`json_value`, which is ``->>``.
    """

    name = "json_get"
    type = JSON()
    inherit_cache = True

    def __init__(self, column: Any, key: str) -> None:
        super().__init__(_json(column), _text(key), _text(_json_path(key)))


@compiles(json_get, "postgresql")
def _json_get_postgresql(element: json_get, compiler: Any, **kwargs: Any) -> str:
    column, key, _path = _operands(element)
    got = sa.type_coerce(column, postgresql.JSONB).op(
        "->", return_type=postgresql.JSONB
    )(key)
    return compiler.process(_pg_group(got), **kwargs)


@compiles(json_get)
def _json_get_default(element: json_get, compiler: Any, **kwargs: Any) -> str:
    column, _key, path = _operands(element)
    return compiler.process(sa.func.json_extract(column, path), **kwargs)


class json_has_key(FunctionElement[bool]):
    """Whether a JSON object has ``key`` at the top level.

    On postgres this is the ``?`` existence operator. Unlike
    :class:`json_has_any_key` and :class:`json_has_all_keys` this addresses
    object *keys* on every backend, SQLite included.
    """

    name = "json_has_key"
    type: Any = sa.Boolean()
    inherit_cache = True

    def __init__(self, column: Any, key: str) -> None:
        super().__init__(_json(column), _text(key), _text(_json_path(key)))


@compiles(json_has_key, "postgresql")
def _json_has_key_postgresql(
    element: json_has_key, compiler: Any, **kwargs: Any
) -> str:
    column, key, _path = _operands(element)
    return compiler.process(
        sa.type_coerce(column, postgresql.JSONB).has_key(key), **kwargs
    )


@compiles(json_has_key, "sqlite")
def _json_has_key_sqlite(element: json_has_key, compiler: Any, **kwargs: Any) -> str:
    column, _key, path = _operands(element)
    return compiler.process(sa.func.json_type(column, path).is_not(None), **kwargs)


@compiles(json_has_key, "mysql")
@compiles(json_has_key)
def _json_has_key_mysql(element: json_has_key, compiler: Any, **kwargs: Any) -> str:
    column, _key, path = _operands(element)
    return compiler.process(
        sa.func.json_contains_path(column, _text("one"), path), **kwargs
    )


class json_array_length(FunctionElement[int]):
    """How many elements a JSON array has.

    Only arrays are portable here: given an object, postgres raises while
    SQLite answers 0 and MySQL answers 1.
    """

    name = "json_array_length"
    type = sa.Integer()
    inherit_cache = True

    def __init__(self, column: Any) -> None:
        super().__init__(_json(column))


@compiles(json_array_length, "postgresql")
def _json_array_length_postgresql(
    element: json_array_length, compiler: Any, **kwargs: Any
) -> str:
    (column,) = _operands(element)
    return compiler.process(
        sa.func.jsonb_array_length(sa.type_coerce(column, postgresql.JSONB)), **kwargs
    )


@compiles(json_array_length, "sqlite")
def _json_array_length_sqlite(
    element: json_array_length, compiler: Any, **kwargs: Any
) -> str:
    (column,) = _operands(element)
    return compiler.process(sa.func.json_array_length(column), **kwargs)


@compiles(json_array_length, "mysql")
@compiles(json_array_length)
def _json_array_length_mysql(
    element: json_array_length, compiler: Any, **kwargs: Any
) -> str:
    (column,) = _operands(element)
    return compiler.process(sa.func.json_length(column), **kwargs)


class json_keys(FunctionElement[Any]):
    """The top level keys of a JSON object, as a JSON array of strings."""

    name = "json_keys"
    type = JSON()
    inherit_cache = True

    def __init__(self, column: Any) -> None:
        super().__init__(_json(column))


@compiles(json_keys, "postgresql")
def _json_keys_postgresql(element: json_keys, compiler: Any, **kwargs: Any) -> str:
    (column,) = _operands(element)
    keys = sa.func.jsonb_object_keys(sa.type_coerce(column, postgresql.JSONB)).alias(
        "json_keys"
    )
    # jsonb_object_keys is set returning, and jsonb_agg over no rows is NULL
    aggregated = (
        sa.select(sa.func.jsonb_agg(sa.literal_column("json_keys")))
        .select_from(keys)
        .scalar_subquery()
    )
    return compiler.process(
        sa.func.coalesce(aggregated, _pg_json(_text("[]"))), **kwargs
    )


@compiles(json_keys, "sqlite")
def _json_keys_sqlite(element: json_keys, compiler: Any, **kwargs: Any) -> str:
    (column,) = _operands(element)
    each = sa.func.json_each(column).alias("json_each")
    return compiler.process(
        sa.select(sa.func.json_group_array(sa.literal_column("json_each.key")))
        .select_from(each)
        .scalar_subquery(),
        **kwargs,
    )


@compiles(json_keys, "mysql")
@compiles(json_keys)
def _json_keys_mysql(element: json_keys, compiler: Any, **kwargs: Any) -> str:
    (column,) = _operands(element)
    return compiler.process(sa.func.json_keys(column), **kwargs)
