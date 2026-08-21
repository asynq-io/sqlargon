from typing import TypeVar

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon.mixins import CreatedUpdatedMixin, UUIDV7ModelMixin
from sqlargon.orm import Base

from .mixins import (
    AttributesMixin,
    EmbeddingMixin,
    TextMixin,
    VectorCollectionMixin,
)


class EmbeddingBase(EmbeddingMixin, Base):
    """Declarative base for models carrying an embedding column.

    The minimum :class:`~sqlargon.vectors.VectorRepository` needs. Add
    only the further mixins the application wants::

        class Document(UUIDV7ModelMixin, EmbeddingBase):
            __vector_dim__ = 384
    """

    __abstract__ = True


VectorModel = TypeVar("VectorModel", bound=EmbeddingBase)


class TextBase(TextMixin, Base):
    """Declarative base for models searchable by full text alone.

    What :class:`~sqlargon.vectors.TextSearchRepository` needs; combine
    with :class:`EmbeddingBase` -- or inherit
    :class:`TextEmbeddingBase` -- to also search by similarity.
    """

    __abstract__ = True


TextModel = TypeVar("TextModel", bound=TextBase)


class TextEmbeddingBase(TextMixin, EmbeddingBase):
    """Declarative base for models searchable by similarity and by text.

    What :class:`~sqlargon.vectors.HybridVectorRepository` needs, so its
    ``rrf_search`` has both rankings to fuse.
    """

    __abstract__ = True


HybridModel = TypeVar("HybridModel", bound=TextEmbeddingBase)


class VectorCollection(UUIDV7ModelMixin, CreatedUpdatedMixin, Base):
    """A named grouping of vector documents.

    The default target of
    :class:`~sqlargon.vectors.VectorCollectionMixin`; importing it
    registers the table with the shared metadata, so ``create_all()``
    creates it.
    """

    name: Mapped[str] = mapped_column(sa.Unicode(255), unique=True, nullable=False)


class VectorDocument(
    UUIDV7ModelMixin,
    CreatedUpdatedMixin,
    AttributesMixin,
    VectorCollectionMixin,
    TextEmbeddingBase,
):
    """Ready-made document model: embedding, text, attributes, collection.

    The batteries-included option -- subclass it, set ``__vector_dim__``
    and add the indexes the columns deserve::

        class Document(VectorDocument):
            __vector_dim__ = 384

            @declared_attr.directive
            def __table_args__(cls) -> tuple[sa.Index, ...]:
                return (cls.embedding_index(), cls.attributes_index())

    Compose the mixins directly instead when only some of the columns
    are wanted.
    """

    __abstract__ = True
