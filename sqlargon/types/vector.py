from __future__ import annotations

import struct
from enum import Enum
from typing import Any, ClassVar

import sqlalchemy as sa
from sqlalchemy import Dialect, FunctionElement, TypeDecorator
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import coercions, roles
from sqlalchemy.types import TypeEngine

from sqlargon.query_builder import UnsupportedDialectError

try:
    from pgvector.sqlalchemy import VECTOR
except ImportError as e:
    msg = "Vector columns require the 'pgvector' package; install 'sqlargon[vectors]'"
    raise ImportError(msg) from e

__all__ = [
    "DistanceMetric",
    "UnsupportedDialectError",
    "Vector",
    "cosine_distance",
    "distance_for",
    "l1_distance",
    "l2_distance",
    "max_inner_product",
]


class DistanceMetric(str, Enum):
    """Distance metric of a vector similarity search.

    Maps each metric to the pgvector operator, the pgvector index
    operator class and the sqlite-vector ``distance`` option.
    """

    COSINE = "cosine"
    L2 = "l2"
    DOT = "dot"
    L1 = "l1"

    @property
    def pg_operator(self) -> str:
        return _PG_OPERATORS[self]

    @property
    def pg_opclass(self) -> str:
        return _PG_OPCLASSES[self]

    @property
    def sqlite_option(self) -> str:
        return _SQLITE_OPTIONS[self]


_PG_OPERATORS = {
    DistanceMetric.COSINE: "<=>",
    DistanceMetric.L2: "<->",
    DistanceMetric.DOT: "<#>",
    DistanceMetric.L1: "<+>",
}

_PG_OPCLASSES = {
    DistanceMetric.COSINE: "vector_cosine_ops",
    DistanceMetric.L2: "vector_l2_ops",
    DistanceMetric.DOT: "vector_ip_ops",
    DistanceMetric.L1: "vector_l1_ops",
}

_SQLITE_OPTIONS = {
    DistanceMetric.COSINE: "COSINE",
    DistanceMetric.L2: "L2",
    DistanceMetric.DOT: "DOT",
    DistanceMetric.L1: "L1",
}


class _vector_distance(FunctionElement):
    """Distance between two vectors, ordered ascending by similarity.

    Compiles to the matching pgvector operator on PostgreSQL and raises
    :class:`UnsupportedDialectError` elsewhere -- sqlite-vector exposes no
    scalar distance function, use
    :meth:`~sqlargon.vectors.VectorRepository.search` there.
    """

    type = sa.Float()
    metric: ClassVar[DistanceMetric]
    inherit_cache = True

    def __init__(self, left: Any, right: Any) -> None:
        # both operands are coerced here rather than at compile time, so
        # they belong to the element's own clause list -- that is what lets
        # the statement be cached and still bind a fresh vector per
        # execution. The query vector is typed after the column it is
        # compared with, so a plain list of floats binds as a vector.
        column = coercions.expect(roles.ExpressionElementRole, left)
        super().__init__(
            column,
            coercions.expect(roles.ExpressionElementRole, right, type_=column.type),
        )

    @property
    def operands(self) -> tuple[sa.ColumnElement[Any], sa.ColumnElement[Any]]:
        left, right = self.clauses
        return left, right


class cosine_distance(_vector_distance):
    name = "cosine_distance"
    metric = DistanceMetric.COSINE
    inherit_cache = True


class l2_distance(_vector_distance):
    name = "l2_distance"
    metric = DistanceMetric.L2
    inherit_cache = True


class max_inner_product(_vector_distance):
    """Negative inner product, so ascending order means most similar first."""

    name = "max_inner_product"
    metric = DistanceMetric.DOT
    inherit_cache = True


class l1_distance(_vector_distance):
    name = "l1_distance"
    metric = DistanceMetric.L1
    inherit_cache = True


_DISTANCE_ELEMENTS: dict[DistanceMetric, type[_vector_distance]] = {
    element.metric: element
    for element in (cosine_distance, l2_distance, max_inner_product, l1_distance)
}


def distance_for(metric: DistanceMetric) -> type[_vector_distance]:
    """The distance element matching ``metric``."""
    return _DISTANCE_ELEMENTS[metric]


@compiles(cosine_distance, "postgresql")
@compiles(l2_distance, "postgresql")
@compiles(max_inner_product, "postgresql")
@compiles(l1_distance, "postgresql")
def _distance_postgresql(
    element: _vector_distance, compiler: Any, **kwargs: Any
) -> str:
    left, right = element.operands
    return compiler.process(
        left.op(element.metric.pg_operator, return_type=sa.Float)(right), **kwargs
    )


@compiles(cosine_distance)
@compiles(l2_distance)
@compiles(max_inner_product)
@compiles(l1_distance)
def _distance_default(element: _vector_distance, compiler: Any, **_kwargs: Any) -> str:
    msg = (
        f"{element.name} is not supported on the {compiler.dialect.name!r} dialect; "
        "on sqlite use VectorRepository.search()"
    )
    raise UnsupportedDialectError(msg)


class Vector(TypeDecorator):
    """Embedding column of a fixed dimension.

    ``pgvector.VECTOR`` on PostgreSQL, a little-endian float32 BLOB on
    SQLite (the layout sqlite-vector's ``FLOAT32`` columns expect) and
    plain JSON storage on other dialects. Values are read back as
    ``list[float]`` everywhere.
    """

    impl = VECTOR
    cache_ok = True

    def __init__(self, dim: int) -> None:
        super().__init__()
        self.dim = dim

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(VECTOR(self.dim))
        if dialect.name == "sqlite":
            return dialect.type_descriptor(sa.LargeBinary())
        return dialect.type_descriptor(sa.JSON(none_as_null=True))

    def process_bind_param(self, value: Any, dialect: Dialect) -> Any:
        if value is None:
            return None
        if dialect.name == "sqlite":
            return struct.pack(f"<{len(value)}f", *value)
        if isinstance(value, list):
            return value
        return [float(item) for item in value]

    def process_result_value(self, value: Any, dialect: Dialect) -> list[float] | None:
        if value is None:
            return None
        if dialect.name == "sqlite":
            return list(struct.unpack(f"<{len(value) // 4}f", value))
        if isinstance(value, list):
            return value
        return [float(item) for item in value]

    class ComparatorFactory(TypeEngine.Comparator):
        def cosine_distance(self, other: Any) -> cosine_distance:
            return cosine_distance(self.expr, other)

        def l2_distance(self, other: Any) -> l2_distance:
            return l2_distance(self.expr, other)

        def max_inner_product(self, other: Any) -> max_inner_product:
            return max_inner_product(self.expr, other)

        def l1_distance(self, other: Any) -> l1_distance:
            return l1_distance(self.expr, other)

        def distance(self, other: Any, metric: DistanceMetric) -> _vector_distance:
            return distance_for(metric)(self.expr, other)

    comparator_factory = ComparatorFactory
