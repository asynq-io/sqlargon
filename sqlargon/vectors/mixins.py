from typing import TYPE_CHECKING, Any, ClassVar
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, declared_attr, mapped_column

from sqlargon.types import GUID, JSON
from sqlargon.types.json import json_contains
from sqlargon.types.vector import DistanceMetric, Vector


class EmbeddingMixin:
    """Adds a fixed-dimension ``embedding`` column to a model.

    Override ``__vector_dim__`` and ``__vector_distance__`` on the
    concrete subclass to size the column and pick the metric its index
    and default searches use::

        class Document(EmbeddingMixin, Base):
            __vector_dim__ = 384
            __vector_distance__ = DistanceMetric.L2
    """

    __vector_dim__: ClassVar[int] = 1536
    __vector_distance__: ClassVar[DistanceMetric] = DistanceMetric.COSINE

    @declared_attr
    def embedding(cls) -> Mapped[list[float]]:
        return mapped_column(Vector(cls.__vector_dim__), nullable=False)

    @classmethod
    def embedding_index(
        cls,
        name: str | None = None,
        *,
        m: int = 16,
        ef_construction: int = 64,
    ) -> sa.Index:
        """An HNSW index over ``embedding``, tuned for ``__vector_distance__``.

        Add it to the concrete model's ``__table_args__``. The DDL only
        runs on PostgreSQL -- sqlite-vector needs no index. With ``name``
        omitted the metadata naming convention applies.
        """
        return sa.Index(
            name,
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": m, "ef_construction": ef_construction},
            postgresql_ops={"embedding": cls.__vector_distance__.pg_opclass},
        ).ddl_if(dialect="postgresql")


class TextMixin:
    """Adds a nullable ``text`` column and its full-text search expressions.

    ``__text_regconfig__`` is the PostgreSQL text search configuration
    the index and the search ranking share, so overriding it moves both::

        class Document(TextMixin, Base):
            __text_regconfig__ = "english"
    """

    if TYPE_CHECKING:
        __tablename__: str

    __text_regconfig__: ClassVar[str] = "simple"

    text: Mapped[str | None] = mapped_column(sa.Text(), nullable=True)

    @classmethod
    def _regconfig(cls) -> sa.ColumnElement[Any]:
        """The search configuration, inlined rather than bound.

        Index DDL cannot carry bind parameters, and the planner only uses
        a functional index when the query spells the expression exactly
        as the index does -- so both have to inline it.
        """
        regconfig = cls.__text_regconfig__
        if not regconfig.replace("_", "").isalnum():
            msg = f"{regconfig!r} is not a valid text search configuration name"
            raise ValueError(msg)
        return sa.literal_column(f"'{regconfig}'")

    @classmethod
    def text_document(cls) -> sa.ColumnElement[Any]:
        """The ``tsvector`` of ``text``, as indexed and as searched."""
        return sa.func.to_tsvector(cls._regconfig(), cls.text)

    @classmethod
    def text_query(cls, query: str) -> sa.ColumnElement[Any]:
        """``query`` parsed as a web search style ``tsquery``."""
        return sa.func.websearch_to_tsquery(cls._regconfig(), query)

    @classmethod
    def text_index(cls, name: str | None = None) -> sa.Index:
        """A GIN index over :meth:`text_document`, PostgreSQL only.

        Named explicitly rather than by the metadata convention, which
        would derive the name from the inlined search configuration
        instead of the column.
        """
        return sa.Index(
            name or f"ix_{cls.__tablename__}__text",
            cls.text_document(),
            postgresql_using="gin",
        ).ddl_if(dialect="postgresql")


class AttributesMixin:
    """Adds an ``attributes`` JSON column for application metadata."""

    # the default is parenthesised because MySQL accepts one on a JSON
    # column only as an expression, and every other backend reads
    # ``DEFAULT ('{}')`` the same way as a bare literal
    attributes: Mapped[dict[str, Any]] = mapped_column(
        JSON(), nullable=False, default=dict, server_default=sa.text("('{}')")
    )

    @classmethod
    def attributes_contain(cls, value: dict[str, Any]) -> sa.ColumnElement[bool]:
        """Whether ``attributes`` contains every given key and value.

        Pass it to any search as an ordinary filter::

            await docs.search(vector, Document.attributes_contain({"lang": "en"}))
        """
        return json_contains(cls.attributes, value)

    @classmethod
    def attributes_index(cls, name: str | None = None) -> sa.Index:
        """A GIN index over ``attributes``, PostgreSQL only."""
        return sa.Index(
            name,
            "attributes",
            postgresql_using="gin",
            postgresql_ops={"attributes": "jsonb_path_ops"},
        ).ddl_if(dialect="postgresql")


class VectorCollectionMixin:
    """Adds a nullable ``collection_id`` foreign key.

    Points at :class:`~sqlargon.vectors.VectorCollection` by default;
    override ``__collection_table__`` to group documents by a table of
    your own.
    """

    __collection_table__: ClassVar[str] = "vector_collection"

    @declared_attr
    def collection_id(cls) -> Mapped[UUID | None]:
        return mapped_column(
            GUID(),
            sa.ForeignKey(f"{cls.__collection_table__}.id", ondelete="CASCADE"),
            nullable=True,
            index=True,
        )
