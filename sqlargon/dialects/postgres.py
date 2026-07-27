from __future__ import annotations

from hashlib import md5
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import Insert, insert
from typing_extensions import assert_never

from sqlargon.query_builder import Option, QueryBuilder

if TYPE_CHECKING:
    from sqlalchemy.sql._typing import _DMLTableArgument

    from sqlargon.typing import OnConflict, Values

INT64_SIZE = 2**63 - 1


def _key_to_int(key: str) -> int:
    return (
        int(md5(key.encode("utf-8"), usedforsecurity=False).hexdigest(), 16)
        % INT64_SIZE
    )


class PostgresqlQueryBuilder(QueryBuilder):
    supported_options = Option.RETURNING | Option.CONFLICTS | Option.LOCKS
    _lock_query = sa.text("SELECT pg_advisory_lock(:key)")
    _unlock_query = sa.text("SELECT pg_advisory_unlock(:key)")

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
            kw = {
                k: on_conflict.options.get(k)
                for k in ("constraint", "index_elements", "index_where")
            }
            if on_conflict.do == "update":
                to_set = {
                    k
                    for k in on_conflict.options.get("set_", [])
                    if k not in on_conflict.options.get("exclude_set", [])
                }
                set_ = {k: getattr(query.excluded, k) for k in to_set}
                query = query.on_conflict_do_update(
                    **kw,
                    set_=set_,
                    where=on_conflict.options.get("where"),
                )
            elif on_conflict.do == "ignore":
                query = query.on_conflict_do_nothing(**kw)
            else:
                assert_never(on_conflict.do)
        return query

    def lock(self, key: str) -> sa.TextClause:
        int_key = _key_to_int(key)
        return self._lock_query.bindparams(key=int_key)

    def unlock(self, key: str) -> sa.TextClause:
        int_key = _key_to_int(key)
        return self._unlock_query.bindparams(key=int_key)
