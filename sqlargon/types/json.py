import sqlalchemy as sa
from sqlalchemy import BOOLEAN, FunctionElement, TypeDecorator
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.ext.compiler import compiles

from ..utils import json_dumps


class json_contains(FunctionElement):
    """
    Platform independent json_contains operator, tests if the
    `left` expression contains the `right` expression.

    On postgres this is equivalent to the @> containment operator.
    https://www.postgresql.org/docs/current/functions-json.html
    """

    type = BOOLEAN
    name = "json_contains"
    inherit_cache = False

    def __init__(self, left, right):
        self.left = left
        self.right = right
        super().__init__()


@compiles(json_contains, "postgresql")
def _json_contains_postgresql(element, compiler, **kwargs):
    return compiler.process(
        sa.type_coerce(element.left, postgresql.JSONB).contains(
            sa.type_coerce(element.right, postgresql.JSONB)
        ),
        **kwargs,
    )


def _json_contains_sqlite_fn(left, right, compiler, **kwargs):
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
def _json_contains_sqlite(element, compiler, **kwargs):
    return _json_contains_sqlite_fn(element.left, element.right, compiler, **kwargs)


class json_has_any_key(FunctionElement):
    """
    Platform independent json_has_any_key operator.

    On postgres this is equivalent to the ?| existence operator.
    https://www.postgresql.org/docs/current/functions-json.html
    """

    type = BOOLEAN  # type: ignore
    name = "json_has_any_key"
    inherit_cache = False

    def __init__(self, json_expr, values: list):
        self.json_expr = json_expr
        if not all(isinstance(v, str) for v in values):
            raise ValueError("json_has_any_key values must be strings")
        self.values = values
        super().__init__()


@compiles(json_has_any_key, "postgresql")
@compiles(json_has_any_key)
def _json_has_any_key_postgresql(element, compiler, **kwargs):
    values_array = postgresql.array(element.values)
    # if the array is empty, postgres requires a type annotation
    if not element.values:
        values_array = sa.cast(values_array, postgresql.ARRAY(sa.String))

    return compiler.process(
        sa.type_coerce(element.json_expr, postgresql.JSONB).has_any(values_array),
        **kwargs,
    )


@compiles(json_has_any_key, "sqlite")
def _json_has_any_key_sqlite(element, compiler, **kwargs):
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


class json_has_all_keys(FunctionElement):
    """Platform independent json_has_all_keys operator.

    On postgres this is equivalent to the ?& existence operator.
    https://www.postgresql.org/docs/current/functions-json.html
    """

    type = BOOLEAN  # type: ignore
    name = "json_has_all_keys"
    inherit_cache = False

    def __init__(self, json_expr, values: list):
        self.json_expr = json_expr
        if isinstance(values, list) and not all(isinstance(v, str) for v in values):
            raise ValueError(
                "json_has_all_key values must be strings if provided as a literal list"
            )
        self.values = values
        super().__init__()


@compiles(json_has_all_keys, "postgresql")
@compiles(json_has_all_keys)
def _json_has_all_keys_postgresql(element, compiler, **kwargs):
    values_array = postgresql.array(element.values)

    # if the array is empty, postgres requires a type annotation
    if not element.values:
        values_array = sa.cast(values_array, postgresql.ARRAY(sa.String))

    return compiler.process(
        sa.type_coerce(element.json_expr, postgresql.JSONB).has_all(values_array),
        **kwargs,
    )


@compiles(json_has_all_keys, "sqlite")
def _json_has_all_keys_sqlite(element, compiler, **kwargs):
    # "has all keys" is equivalent to "json contains"
    return _json_contains_sqlite_fn(
        left=element.json_expr,
        right=element.values,
        compiler=compiler,
        **kwargs,
    )


class JSON(TypeDecorator):
    """
    JSON type that returns SQLAlchemy's dialect-specific JSON types, where
    possible. Uses generic JSON otherwise.

    The "base" type is postgresql.JSONB to expose useful methods prior
    to SQL compilation
    """

    impl = postgresql.JSONB
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(postgresql.JSONB(none_as_null=True))
        elif dialect.name == "sqlite":
            return dialect.type_descriptor(sqlite.JSON(none_as_null=True))
        else:
            return dialect.type_descriptor(sa.JSON(none_as_null=True))

    class ComparatorFactory(sa.JSON.Comparator):
        def contains(self, other, **kw):
            return json_contains(self, other)

        def has_any_key(self, other):
            return json_has_any_key(self, other)

        def has_all_keys(self, other):
            return json_has_all_keys(self, other)

    comparator_factory = ComparatorFactory
