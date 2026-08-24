from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import Insert, insert
from typing_extensions import assert_never

from sqlargon.query_builder import Option, QueryBuilder, UnsupportedDialectError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.sql._typing import _ColumnExpressionArgument, _DMLTableArgument

    from sqlargon.types.vector import DistanceMetric
    from sqlargon.typing import OnConflict, Values


_SQLITE_OPTIONS = Option.CONFLICTS | Option.VECTORS

if sqlite3.sqlite_version > "3.35":
    _SQLITE_OPTIONS |= Option.RETURNING


class SQLiteQueryBuilder(QueryBuilder):
    """Query builder for SQLite, searching vectors through sqlite-vector.

    That extension exposes no scalar distance function, so a search joins
    the table valued scan it does expose rather than ordering by an
    expression, and the column has to be declared to it per connection --
    see :meth:`vector_init`.
    """

    supported_options = _SQLITE_OPTIONS

    def excluded(self, table: _DMLTableArgument) -> Any:
        return insert(table).excluded

    def _insert(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        on_conflict: OnConflict | None = None,
    ) -> Insert:
        query = insert(table)
        if values:
            query = query.values(values)
        if on_conflict:
            index_elements = on_conflict.options.get("index_elements")
            index_where = on_conflict.options.get("index_where")
            if on_conflict.do == "update":
                query = query.on_conflict_do_update(
                    index_elements=index_elements,
                    index_where=index_where,
                    set_=self.conflict_set(on_conflict.options, query.excluded),
                    where=on_conflict.options.get("where"),
                )
            elif on_conflict.do == "ignore":
                query = query.on_conflict_do_nothing(
                    index_elements=index_elements,
                    index_where=index_where,
                )
            else:
                assert_never(on_conflict.do)
        return query

    def vector_search(
        self,
        model: Any,
        embedding: Sequence[float],
        *filters: _ColumnExpressionArgument[bool],
        limit: int,
        metric: DistanceMetric | None = None,
    ) -> sa.Select[Any]:
        """A join against the streaming scan, so filters cannot under-return.

        ``vector_full_scan`` is called without ``k``: a top-k scan would
        pick its rows before the ``WHERE`` clause ran, and could then
        return fewer than ``limit`` of them.
        """
        if metric is not None and metric is not model.__vector_distance__:
            msg = (
                "sqlite-vector fixes the distance metric per column; "
                f"{model.__name__} uses {model.__vector_distance__.value!r}"
            )
            raise UnsupportedDialectError(msg)
        table = model.__table__
        scan = sa.func.vector_full_scan(
            sa.literal(table.name, sa.String),
            sa.literal("embedding", sa.String),
            self.query_vector(model, embedding),
        ).table_valued("rowid", "distance")
        rowid = sa.literal_column(f'"{table.name}".rowid')
        return (
            sa.select(model, scan.c.distance)
            .select_from(sa.join(table, scan, rowid == scan.c.rowid))
            .where(*filters)
            .order_by(scan.c.distance)
            .limit(limit)
        )

    def vector_init(self, model: Any) -> sa.Executable:
        """Declare the embedding column to sqlite-vector.

        Its dimension and metric are fixed here rather than in the schema,
        which is why the statement has to run on every connection that
        searches.
        """
        options = (
            f"type=FLOAT32,dimension={model.__vector_dim__},"
            f"distance={model.__vector_distance__.sqlite_option}"
        )
        return sa.select(
            sa.func.vector_init(
                sa.literal(model.__table__.name, sa.String),
                sa.literal("embedding", sa.String),
                sa.literal(options, sa.String),
            )
        )
