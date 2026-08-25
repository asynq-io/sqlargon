from __future__ import annotations

from .expression import get_locale
from .translation import LocaleMap, Translation, as_translation, select_current


class TranslationMixin:
    """Multi-locale read and write helpers shared by both backends.

    Plain attribute access stays transparent: ``model.title`` reads as the text
    of the active locale, while these helpers reach the other locales. Writing
    merges -- no write drops a locale it does not name, except
    `clear_translations`.
    """

    def get_translations(self, field: str) -> LocaleMap:
        """Return every known translation of ``field``, keyed by locale."""
        translation = as_translation(getattr(self, field))
        return {} if translation is None else translation.data

    def get_translation(self, field: str, locale: str | None = None) -> str | None:
        """Return the text of ``field`` for ``locale``, the active one by default.

        An explicit ``locale`` is looked up as given; only the active locale
        walks its fallback chain.
        """
        data = self.get_translations(field)
        if locale is None:
            return select_current(data)
        return data.get(locale)

    def set_translation(
        self, field: str, value: str, locale: str | None = None
    ) -> None:
        """Store ``value`` under ``locale``, keeping the other translations."""
        data = self.get_translations(field)
        data[locale or get_locale()] = value
        setattr(self, field, Translation(select_current(data) or value, data))

    def clear_translations(self, field: str) -> None:
        """Drop every translation of ``field``, leaving it empty rather than unset.

        The column keeps holding a translation -- an empty one -- so a model may
        declare it non-nullable.
        """
        setattr(self, field, Translation("", {}))
