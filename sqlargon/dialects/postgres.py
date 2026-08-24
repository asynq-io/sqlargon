from __future__ import annotations

from hashlib import md5
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import Insert, insert
from typing_extensions import assert_never

from sqlargon.query_builder import Option, QueryBuilder

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.sql._typing import _ColumnExpressionArgument, _DMLTableArgument

    from sqlargon.types.vector import DistanceMetric
    from sqlargon.typing import OnConflict, Values

INT64_SIZE = 2**63 - 1


def _key_to_int(key: str) -> int:
    return (
        int(md5(key.encode("utf-8"), usedforsecurity=False).hexdigest(), 16)
        % INT64_SIZE
    )


class PostgresqlQueryBuilder(QueryBuilder):
    supported_options = (
        Option.RETURNING
        | Option.CONFLICTS
        | Option.LOCKS
        | Option.VECTORS
        | Option.FULL_TEXT
    )
    _lock_query = sa.text("SELECT pg_advisory_lock(:key)")
    _unlock_query = sa.text("SELECT pg_advisory_unlock(:key)")

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
            kw = {
                k: on_conflict.options.get(k)
                for k in ("constraint", "index_elements", "index_where")
            }
            if on_conflict.do == "update":
                query = query.on_conflict_do_update(
                    **kw,
                    set_=self.conflict_set(on_conflict.options, query.excluded),
                    where=on_conflict.options.get("where"),
                )
            elif on_conflict.do == "ignore":
                query = query.on_conflict_do_nothing(**kw)
            else:
                assert_never(on_conflict.do)
        return query

    def vector_distance(
        self,
        model: Any,
        embedding: Sequence[float],
        metric: DistanceMetric | None = None,
    ) -> sa.ColumnElement[float]:
        """A pgvector distance operator between the column and ``embedding``."""
        from sqlargon.types.vector import distance_for

        element = distance_for(metric or model.__vector_distance__)
        return element(model.embedding, self.query_vector(model, embedding))

    def vector_search(
        self,
        model: Any,
        embedding: Sequence[float],
        *filters: _ColumnExpressionArgument[bool],
        limit: int,
        metric: DistanceMetric | None = None,
    ) -> sa.Select[Any]:
        distance = self.vector_distance(model, embedding, metric)
        return (
            sa.select(model, distance.label("distance"))
            .where(*filters)
            .order_by(distance)
            .limit(limit)
        )

    def text_score(self, model: Any, query: str) -> sa.ColumnElement[float]:
        """How well the model's text matches ``query``; higher is better."""
        return sa.func.ts_rank(model.text_document(), model.text_query(query))

    def text_match(self, model: Any, query: str) -> sa.ColumnElement[bool]:
        """Whether the model's text matches ``query`` at all."""
        return model.text_document().op("@@")(model.text_query(query))

    def text_search(
        self,
        model: Any,
        query: str,
        *filters: _ColumnExpressionArgument[bool],
        limit: int,
    ) -> sa.Select[Any]:
        score = self.text_score(model, query)
        return (
            sa.select(model, score.label("score"))
            .where(self.text_match(model, query), *filters)
            .order_by(score.desc())
            .limit(limit)
        )

    def _rank_cte(
        self,
        model: Any,
        order_by: sa.ColumnElement[Any],
        filters: Sequence[_ColumnExpressionArgument[bool]],
        *,
        candidates: int,
        name: str,
    ) -> sa.CTE:
        """The ``candidates`` best rows by ``order_by``, numbered from one."""
        return (
            sa.select(
                self.identity_column(model).label("id"),
                sa.func.row_number().over(order_by=order_by).label("rank"),
            )
            .where(*filters)
            .order_by(order_by)
            .limit(candidates)
            .cte(name)
        )

    def rrf_search(  # noqa: PLR0913 -- keyword-only tuning knobs, each with a default
        self,
        model: Any,
        embedding: Sequence[float],
        query: str,
        *filters: _ColumnExpressionArgument[bool],
        k: int = 60,
        limit: int = 10,
        candidates: int = 50,
    ) -> sa.Select[Any]:
        vector_rank = self._rank_cte(
            model,
            self.vector_distance(model, embedding),
            filters,
            candidates=candidates,
            name="vector_candidates",
        )
        text_rank = self._rank_cte(
            model,
            self.text_score(model, query).desc(),
            (self.text_match(model, query), *filters),
            candidates=candidates,
            name="text_candidates",
        )
        score = (
            sa.func.coalesce(1.0 / (k + vector_rank.c.rank), 0.0)
            + sa.func.coalesce(1.0 / (k + text_rank.c.rank), 0.0)
        ).label("score")
        # a full outer join so a row either ranking alone found still scores
        fused = (
            sa.select(
                sa.func.coalesce(vector_rank.c.id, text_rank.c.id).label("id"), score
            )
            .select_from(
                sa.join(
                    vector_rank,
                    text_rank,
                    vector_rank.c.id == text_rank.c.id,
                    full=True,
                )
            )
            .subquery("rrf")
        )
        return (
            sa.select(model, fused.c.score)
            .join(fused, self.identity_column(model) == fused.c.id)
            .order_by(fused.c.score.desc())
            .limit(limit)
        )

    def lock(self, key: str) -> sa.TextClause:
        int_key = _key_to_int(key)
        return self._lock_query.bindparams(key=int_key)

    def unlock(self, key: str) -> sa.TextClause:
        int_key = _key_to_int(key)
        return self._unlock_query.bindparams(key=int_key)
