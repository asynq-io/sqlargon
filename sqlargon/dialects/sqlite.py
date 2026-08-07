from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.sqlite import Insert, insert
from typing_extensions import assert_never

from sqlargon.query_builder import Option, QueryBuilder

if TYPE_CHECKING:
    from sqlalchemy.sql._typing import _DMLTableArgument

    from sqlargon.typing import OnConflict, Values


_SQLITE_OPTIONS = Option.CONFLICTS

if sqlite3.sqlite_version > "3.35":
    _SQLITE_OPTIONS |= Option.RETURNING


class SQLiteQueryBuilder(QueryBuilder):
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
