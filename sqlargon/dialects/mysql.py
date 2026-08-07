from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.dialects.mysql import Insert, insert
from typing_extensions import assert_never

from sqlargon.query_builder import Option, QueryBuilder

if TYPE_CHECKING:
    from sqlalchemy.sql._typing import _DMLTableArgument

    from sqlargon.typing import OnConflict, Values


class MysqlQueryBuilder(QueryBuilder):
    supported_options = Option.RETURNING | Option.CONFLICTS | Option.LOCKS

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
            if on_conflict.do == "ignore":
                query = query.prefix_with("IGNORE")
            elif on_conflict.do == "update":
                query = query.on_duplicate_key_update(
                    **self.conflict_set(on_conflict.options, query.inserted)
                )
            else:
                assert_never(on_conflict.do)
        return query

    def lock(self, key: str) -> sa.TextClause:
        return sa.text("SELECT GET_LOCK(:key, -1)").bindparams(key=key)

    def unlock(self, key: str) -> sa.TextClause:
        return sa.text("SELECT RELEASE_LOCK(:key)").bindparams(key=key)
