from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast
from weakref import WeakKeyDictionary

import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    declared_attr,
    mapped_column,
    relationship,
)
from sqlalchemy.orm.collections import attribute_keyed_dict

from .expression import current_locale
from .mixin import TranslationMixin
from .translation import LocaleMap, Translation, as_translation

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, MutableMapping

    from sqlalchemy import ColumnElement

LOCALE_LENGTH = 10

_translation_classes: MutableMapping[type[Any], type[Any]] = WeakKeyDictionary()


def translation_class(parent: type[Any]) -> type[Any]:
    """Return the translation model registered for ``parent``."""
    for klass in parent.__mro__:
        translation = _translation_classes.get(klass)
        if translation is not None:
            return translation
    msg = f"No translation table declared for {parent.__name__}"
    raise LookupError(msg)


def current_translation(parent: Any) -> Any:
    """Return the relationship joining a model to its active locale row.

    Given an instance instead of the class it reads the attribute, declared
    ``lazy="raise"`` -- it is a join target, never a loader.
    """
    return parent._current_translation  # noqa: SLF001


class TranslationBase:
    """Marker base of every model built by `translation_table`."""

    __translation_parent__: ClassVar[type[Any]]

    if TYPE_CHECKING:
        locale: Mapped[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.__dict__.get("__abstract__"):
            _translation_classes[cls.__translation_parent__] = cls
            _allow_untranslated_fields(cls)


def translation_table(parent: type[Any]) -> type[TranslationBase]:
    """Build the declarative base of ``parent``'s translation table.

    The returned class carries a copy of ``parent``'s primary key -- cascading
    foreign keys back to it -- plus a ``locale`` column, all part of the
    translation table's own primary key. The translated columns declared on the
    concrete subclass are made nullable, whatever their annotation says -- see
    `_allow_untranslated_fields`.
    """
    table = parent.__table__
    namespace: dict[str, Any] = {
        "__abstract__": True,
        "__translation_parent__": parent,
    }
    for column in table.primary_key.columns:
        namespace[column.key] = declared_attr(_parent_key_column(table, column))
    namespace["locale"] = declared_attr(_locale_column)

    base = _declarative_base(parent)
    metaclass: Callable[..., type[Any]] = type(base)
    return cast(
        "type[TranslationBase]",
        metaclass(
            f"{parent.__name__}TranslationBase", (TranslationBase, base), namespace
        ),
    )


def _allow_untranslated_fields(translation: type[Any]) -> None:
    """Make the translated columns of ``translation`` nullable.

    A locale row carries the fields translated to that locale only: writing one
    of them creates the row leaving the others out, and `clear_translations`
    empties them again, so ``NULL`` is how an untranslated field is stored --
    which is what `TranslatableMixin.get_translations` skips over.

    A field the translation table does not define is a typo, raised here so it
    fails where it is declared instead of at flush time, on a column the
    developer never named.
    """
    fields = getattr(translation.__translation_parent__, "__translated_fields__", ())
    for field in fields:
        column = translation.__table__.columns.get(field)
        if column is None:
            msg = f"{translation.__name__} has no column {field!r}"
            raise TypeError(msg)
        column.nullable = True


def _declarative_base(model: type[Any]) -> type[DeclarativeBase]:
    for klass in model.__mro__:
        if DeclarativeBase in klass.__bases__:
            return cast("type[DeclarativeBase]", klass)
    msg = f"{model.__name__} is not a declarative model"
    raise TypeError(msg)


def _locale_column(cls: type[Any]) -> Mapped[str]:  # noqa: ARG001
    return mapped_column(
        "locale", sa.String(LOCALE_LENGTH), primary_key=True, nullable=False
    )


def _parent_key_column(
    table: sa.Table, column: sa.Column[Any]
) -> Callable[[type[Any]], Mapped[Any]]:
    def factory(cls: type[Any]) -> Mapped[Any]:  # noqa: ARG001
        return mapped_column(
            column.key,
            column.type,
            sa.ForeignKey(f"{table.fullname}.{column.key}", ondelete="CASCADE"),
            primary_key=True,
            autoincrement=False,
            nullable=False,
        )

    return factory


def _locale_join(parent: type[Any], locale: ColumnElement[str]) -> ColumnElement[bool]:
    target = translation_class(parent)
    clauses = [
        getattr(parent, column.key) == getattr(target, column.key)
        for column in parent.__table__.primary_key.columns
    ]
    clauses.append(target.locale == locale)
    return sa.and_(*clauses)


class TranslatableMixin(TranslationMixin):
    """Keeps the ``__translated_fields__`` of a model in a translation table.

    Each field becomes a hybrid property reading the text of the active locale
    (walking its fallback chain) and writing to it, creating the locale row on
    demand; assigning ``None`` clears the field in every locale. At class level
    the field resolves to the translation table column, so queries must outer
    join `current_translation` -- see `TranslatableFilterResolver`.

    Reads go through `_translations`, eagerly loaded with the row itself, while
    `_current_translation` is a join target only: it never loads on its own, so
    a joined query costs no extra statement and an async session cannot trip
    over it.
    """

    __translated_fields__: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for field in cls.__translated_fields__:
            setattr(cls, field, _translated_property(field))

    @declared_attr
    @classmethod
    def _translations(cls) -> Mapped[dict[str, Any]]:
        return relationship(
            lambda: translation_class(cls),
            collection_class=attribute_keyed_dict("locale"),
            cascade="all, delete-orphan",
            lazy="selectin",
        )

    @declared_attr
    @classmethod
    def _current_translation(cls) -> Mapped[Any]:
        return relationship(
            lambda: translation_class(cls),
            primaryjoin=lambda: _locale_join(cls, current_locale()),
            uselist=False,
            viewonly=True,
            lazy="raise",
        )

    def get_translations(self, field: str) -> LocaleMap:
        if field not in self.__translated_fields__:
            return super().get_translations(field)
        return {
            locale: value
            for locale, row in self._translations.items()
            if isinstance(value := getattr(row, field, None), str)
        }

    def write_translations(self, field: str, data: Mapping[str, str]) -> None:
        """Write ``field`` for every locale in ``data``, adding missing rows.

        Locales absent from ``data`` keep their text: writes merge, so assigning
        a plain string only touches the active locale. A row created here holds
        ``field`` alone, the other translated columns staying ``NULL`` until
        they are written.
        """
        target = translation_class(type(self))
        for locale, value in data.items():
            row = self._translations.get(locale)
            if row is None:
                row = target(locale=locale)
                self._translations[locale] = row
            setattr(row, field, value)

    def clear_translations(self, field: str) -> None:
        """Drop ``field`` from every locale row, leaving the other fields."""
        for row in self._translations.values():
            setattr(row, field, None)


def _translated_property(field: str) -> hybrid_property[Translation | None]:
    def getter(self: TranslatableMixin) -> Translation | None:
        data = self.get_translations(field)
        return as_translation(data) if data else None

    def setter(self: TranslatableMixin, value: Any) -> None:
        translation = as_translation(value)
        if translation is None:
            self.clear_translations(field)
        else:
            self.write_translations(field, translation.data)

    def expression(cls: type[Any]) -> ColumnElement[Any]:
        return cast("ColumnElement[Any]", getattr(translation_class(cls), field))

    return hybrid_property(getter).setter(setter).expression(expression)
