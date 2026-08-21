from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

from sqlargon.orm import Model
from sqlargon.query_builder import Option, UnsupportedDialectError
from sqlargon.repository import SQLAlchemyRepository

from .mixins import EmbeddingMixin, TextMixin
from .models import HybridModel, TextModel, VectorCollection, VectorModel

if TYPE_CHECKING:
    from collections.abc import Sequence

    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import AsyncSession

    from sqlargon.types.vector import DistanceMetric

_VECTOR_INIT_KEY = "sqlargon_vector_init"


class _SearchRepository(SQLAlchemyRepository[Model], abstract=True):
    """Shared plumbing of the search repositories.

    Each concrete repository declares the mixins its model must carry in
    :meth:`_required_mixins`; anything else raises ``TypeError`` on
    subclassing, the way
    :class:`~sqlargon.repository.SoftDeleteRepository` validates its own.

    The statements themselves are built by the dialect's
    :class:`~sqlargon.query_builder.QueryBuilder`, so what is left here is
    the orchestration: resolving filters, running the statement and
    shaping the rows.
    """

    __slots__ = ()

    def __init_subclass__(cls, *, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(abstract=abstract, **kwargs)
        if abstract:
            return
        for mixin, name in cls._required_mixins():
            if not issubclass(cls.model, mixin):
                msg = (
                    f"{cls.model.__name__} must inherit from {name} "
                    f"to be used with {cls.__name__}"
                )
                raise TypeError(msg)

    @classmethod
    def _required_mixins(cls) -> tuple[tuple[type, str], ...]:
        return ()

    def _filters(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> list[Any]:
        """Positional expressions and keyword equalities, as ``where()`` reads them."""
        filters: list[Any] = list(args)
        filters.extend(
            getattr(self.model, key) == value for key, value in kwargs.items()
        )
        return filters

    def _require(self, option: Option, feature: str) -> None:
        """Refuse a feature the dialect's query builder does not claim."""
        if not self.qb.supports(option):
            msg = f"{feature} is not supported on the {self.db.dialect!r} dialect"
            raise UnsupportedDialectError(msg)


class VectorRepository(_SearchRepository[VectorModel], abstract=True):
    """Similarity search over a model's embedding column.

    The model type variable is bound to
    :class:`~sqlargon.vectors.EmbeddingBase`, so a type checker rejects a
    model without an embedding. At runtime
    :class:`~sqlargon.vectors.EmbeddingMixin` is enough.
    """

    __slots__ = ()

    @classmethod
    def _required_mixins(cls) -> tuple[tuple[type, str], ...]:
        return ((EmbeddingMixin, "EmbeddingMixin"),)

    def distance(
        self, embedding: Sequence[float], metric: DistanceMetric | None = None
    ) -> sa.ColumnElement[float]:
        """The distance between the model's embedding and ``embedding``.

        PostgreSQL only -- sqlite-vector exposes no scalar distance
        function, use :meth:`search` there.
        """
        self._require(Option.VECTORS, "a distance expression")
        return self.qb.vector_distance(self.model, embedding, metric)

    @overload
    async def search(
        self,
        embedding: Sequence[float],
        *filters: Any,
        limit: int = ...,
        metric: DistanceMetric | None = ...,
        with_distance: Literal[False] = ...,
        **kwargs: Any,
    ) -> Sequence[VectorModel]: ...

    @overload
    async def search(
        self,
        embedding: Sequence[float],
        *filters: Any,
        limit: int = ...,
        metric: DistanceMetric | None = ...,
        with_distance: Literal[True],
        **kwargs: Any,
    ) -> list[tuple[VectorModel, float]]: ...

    async def search(
        self,
        embedding: Sequence[float],
        *filters: Any,
        limit: int = 10,
        metric: DistanceMetric | None = None,
        with_distance: bool = False,
        **kwargs: Any,
    ) -> Sequence[VectorModel] | list[tuple[VectorModel, float]]:
        """The ``limit`` models nearest to ``embedding``, most similar first.

        Positional expressions and keyword equalities narrow the search
        the way :meth:`where` does, which is what makes it hybrid::

            await docs.search(
                vector, Document.attributes_contain({"lang": "en"}), limit=5
            )

        ``metric`` overrides the model's distance metric per query on
        PostgreSQL; SQLite fixes the metric per column, so passing a
        different one there raises. ``with_distance=True`` returns
        ``(model, distance)`` pairs.
        """
        self._require(Option.VECTORS, "vector search")
        query = self.qb.vector_search(
            self.model,
            embedding,
            *self._filters(filters, kwargs),
            limit=limit,
            metric=metric,
        )
        result = await self._execute_search(query)
        if with_distance:
            return [(row[0], row[1]) for row in result.all()]
        return result.scalars().all()

    async def _execute_search(self, query: sa.Select[Any]) -> sa.Result[Any]:
        """Run ``query``, declaring the column first where that is needed.

        The declaration has to reach the same connection as the search, so
        both go through one session.
        """
        statement = self.qb.vector_init(self.model)
        if statement is None:
            return await self.execute_query(query)
        async with self.session(query) as session:
            await self._init_vector_column(session, statement)
            return await session.execute(query)

    async def _init_vector_column(
        self, session: AsyncSession, statement: sa.Executable
    ) -> None:
        """Run the declaration once per connection, not once per search."""
        connection = await session.connection()
        info = (await connection.get_raw_connection()).info
        key = f"{_VECTOR_INIT_KEY}:{self.model.__table__.name}"
        if info.get(key):
            return
        await connection.execute(statement)
        info[key] = True


class TextSearchRepository(_SearchRepository[TextModel], abstract=True):
    """Full-text search over a model's text column, PostgreSQL only."""

    __slots__ = ()

    @classmethod
    def _required_mixins(cls) -> tuple[tuple[type, str], ...]:
        return ((TextMixin, "TextMixin"),)

    @overload
    async def text_search(
        self,
        query: str,
        *filters: Any,
        limit: int = ...,
        with_score: Literal[False] = ...,
        **kwargs: Any,
    ) -> Sequence[TextModel]: ...

    @overload
    async def text_search(
        self,
        query: str,
        *filters: Any,
        limit: int = ...,
        with_score: Literal[True],
        **kwargs: Any,
    ) -> list[tuple[TextModel, float]]: ...

    async def text_search(
        self,
        query: str,
        *filters: Any,
        limit: int = 10,
        with_score: bool = False,
        **kwargs: Any,
    ) -> Sequence[TextModel] | list[tuple[TextModel, float]]:
        """The ``limit`` best full-text matches of ``query``, best first.

        Positional expressions and keyword equalities narrow the search
        the way :meth:`where` does. PostgreSQL only.
        """
        self._require(Option.FULL_TEXT, "text_search")
        statement = self.qb.text_search(
            self.model, query, *self._filters(filters, kwargs), limit=limit
        )
        result = await self.execute_query(statement)
        if with_score:
            return [(row[0], row[1]) for row in result.all()]
        return result.scalars().all()


class HybridVectorRepository(
    VectorRepository[HybridModel], TextSearchRepository[HybridModel], abstract=True
):
    """Similarity search, full-text search, and their fusion.

    Requires a model carrying both an embedding and a text column, so
    :meth:`rrf_search` has two rankings to fuse.
    """

    __slots__ = ()

    @classmethod
    def _required_mixins(cls) -> tuple[tuple[type, str], ...]:
        return (
            (EmbeddingMixin, "EmbeddingMixin"),
            (TextMixin, "TextMixin"),
        )

    async def rrf_search(
        self,
        embedding: Sequence[float],
        query: str,
        *filters: Any,
        k: int = 60,
        limit: int = 10,
        candidates: int = 50,
        **kwargs: Any,
    ) -> list[tuple[HybridModel, float]]:
        """Hybrid search fusing the two rankings with reciprocal rank fusion.

        Ranks the ``candidates`` nearest rows and the ``candidates`` best
        full-text matches of ``query``, then scores each row
        ``sum(1 / (k + rank))`` over the rankings it appears in, so a row
        both agree on outranks one either alone prefers. PostgreSQL only.
        """
        self._require(Option.VECTORS | Option.FULL_TEXT, "rrf_search")
        statement = self.qb.rrf_search(
            self.model,
            embedding,
            query,
            *self._filters(filters, kwargs),
            k=k,
            limit=limit,
            candidates=candidates,
        )
        result = await self.execute_query(statement)
        return [(row[0], row[1]) for row in result.all()]


class VectorCollectionRepository(SQLAlchemyRepository[VectorCollection]):
    """Repository over the ready-made :class:`VectorCollection` model."""

    __slots__ = ()
