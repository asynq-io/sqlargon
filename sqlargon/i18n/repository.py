from __future__ import annotations

from typing import TYPE_CHECKING

from sqlargon.repository import SQLAlchemyRepository

from .translatable import TranslatableMixin, TranslatableModel, current_translation

if TYPE_CHECKING:
    from typing import Any

    from typing_extensions import Self


class TranslatedRepository(SQLAlchemyRepository[TranslatableModel], abstract=True):
    """Repository for a model whose fields are backed by a translation table.

    Every ``select()`` outer-joins the active-locale translation row, so
    filtering and ordering on translated columns -- the
    :class:`~sqlalchemy.ext.hybrid.hybrid_property` class-level expressions
    resolve to the translation table's columns -- works without an explicit
    join in the calling code.

    The model type variable is bound to
    :class:`~sqlargon.i18n.TranslatableBase`, so a type checker rejects a
    model that carries no translation table. At runtime the looser
    :class:`~sqlargon.i18n.TranslatableMixin` is enough -- its
    ``_current_translation`` relationship carries the join condition matching
    the model's primary key and the active locale -- and anything else raises
    ``TypeError`` on subclassing.
    """

    def __init_subclass__(cls, *, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(abstract=abstract, **kwargs)
        if abstract:
            return
        if not issubclass(cls.model, TranslatableMixin):
            msg = (
                f"{cls.model.__name__} must inherit from TranslatableMixin "
                f"to be used with {cls.__name__}"
            )
            raise TypeError(msg)

    def select(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        return (
            super()
            .select(*args, **kwargs)
            .join(current_translation(self.model), isouter=True)
        )
