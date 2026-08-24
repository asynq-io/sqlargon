from __future__ import annotations

from typing import TYPE_CHECKING

from sqlargon.repository import SQLAlchemyRepository

if TYPE_CHECKING:
    from typing import Any

    from typing_extensions import Self


class TranslatedRepository(SQLAlchemyRepository, abstract=True):
    """Repository for a model whose fields are backed by a translation table.

    Every ``select()`` outer-joins the active-locale translation row, so
    filtering and ordering on translated columns -- the
    :class:`~sqlalchemy.ext.hybrid.hybrid_property` class-level expressions
    resolve to the translation table's columns -- works without an explicit
    join in the calling code.

    The model must use :class:`TranslatableMixin`, whose
    ``_current_translation`` relationship carries the join condition that
    matches the model's primary key and the active locale.
    """

    def select(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        return (
            super()
            .select(*args, **kwargs)
            .join(
                self.model._current_translation,  # noqa: SLF001
                isouter=True,  # type: ignore[union-attr]
            )
        )
