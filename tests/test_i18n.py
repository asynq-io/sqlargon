"""Unit tests for the two i18n backends, the locale slots and the repository.

Mirrors ``test_outbox.py``: in-memory SQLite via the shared ``db`` fixture,
module-level models to survive ``--count=3`` re-registration. Tests that need
models of their own build a throwaway declarative base -- see `local_base`.
"""

from contextvars import ContextVar

import pytest
import sqlalchemy as sa
from pydantic import BaseModel, TypeAdapter
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.orm import DeclarativeBase

import sqlargon.i18n.expression as _expr
import sqlargon.i18n.translation as _trans
from sqlargon import Base, Database, SQLAlchemyRepository
from sqlargon.i18n import (
    TranslatableBase,
    TranslatableMixin,
    TranslatedRepository,
    TranslatedString,
    Translation,
    TranslationMixin,
    as_translation,
    current_translation,
    fallback_chain,
    get_locale,
    select_current,
    set_fallback_chain,
    set_locale_getter,
    translation_class,
    translation_table,
)
from sqlargon.i18n.translatable import LOCALE_LENGTH, _declarative_base

locale: ContextVar[str] = ContextVar("locale", default="en")

#: The chain each locale walks. A locale absent from the map falls back to
#: itself alone, which is what `select_current` then has to give up on.
CHAINS = {
    "en": ("en",),
    "pl": ("pl", "en"),
    "de": ("de", "en"),
}


def _chain(requested: str | None) -> tuple[str, ...]:
    key = requested or locale.get()
    return CHAINS.get(key, (key,))


def _compile(expr, dialect) -> str:
    """Compile an expression against ``dialect`` without executing it."""
    return str(expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


# --- models ---


class Post(TranslationMixin, Base):
    """The JSON column backend: every locale lives in one ``TranslatedString``."""

    __tablename__ = "test_i18n_post"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    title = sa.Column(TranslatedString(), nullable=True)


class Article(TranslatableBase):
    """The translation table backend, with a plain column alongside."""

    __tablename__ = "test_i18n_article"
    __translated_fields__ = ("title", "body")

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    slug = sa.Column(sa.Unicode(255), nullable=True)


class ArticleTranslation(translation_table(Article)):  # type: ignore[misc]
    __tablename__ = "test_i18n_article_translation"

    # declared NOT NULL on purpose: `_allow_untranslated_fields` overrides it,
    # because a locale row carries only the fields translated to that locale
    title = sa.Column(sa.Unicode(255), nullable=False)
    body = sa.Column(sa.UnicodeText(), nullable=False)


class Untranslated(Base):
    __tablename__ = "test_i18n_untranslated"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)


class PostRepository(SQLAlchemyRepository[Post]):
    pass


class ArticleRepository(TranslatedRepository[Article]):
    pass


# --- fixtures ---


@pytest.fixture(autouse=True)
def _reset_locale_slots():
    """Reset the locale and fallback callable slots before every test."""
    _expr._get_locale = None
    _trans._get_fallback = None
    locale.set("en")


@pytest.fixture
def locales():
    """Configure the slots and hand back the setter switching locale."""
    set_locale_getter(locale.get)
    set_fallback_chain(_chain)
    return locale.set


@pytest.fixture(autouse=True)
async def tables(db: Database):
    created = (
        Post.__table__,
        Article.__table__,
        ArticleTranslation.__table__,
        Untranslated.__table__,
    )
    async with db.engine.begin() as conn:
        for table in created:
            await conn.run_sync(table.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        for table in reversed(created):
            await conn.run_sync(table.drop, checkfirst=True)


@pytest.fixture
def posts():
    return PostRepository()


@pytest.fixture
def articles():
    return ArticleRepository()


@pytest.fixture
def local_base():
    """A throwaway declarative base, so a test can declare models of its own
    without colliding with the shared metadata under ``--count=3``.
    """

    class LocalBase(DeclarativeBase):
        pass

    yield LocalBase
    LocalBase.registry.dispose()


# --- locale getter ---


def test_locale_getter_raises_before_configuration():
    with pytest.raises(RuntimeError, match="No locale getter"):
        get_locale()


def test_locale_getter_returns_the_registered_value():
    set_locale_getter(lambda: "pl")

    assert get_locale() == "pl"


def test_locale_getter_replacing_is_honoured():
    set_locale_getter(lambda: "pl")
    set_locale_getter(lambda: "de")

    assert get_locale() == "de"


def test_locale_getter_slot_is_cleared_between_tests():
    """The autouse fixture clears the slot, so a fresh test starts clean."""
    assert _expr._get_locale is None

    set_locale_getter(lambda: "fr")

    assert get_locale() == "fr"


# --- fallback chain ---


def test_fallback_chain_raises_before_configuration():
    with pytest.raises(RuntimeError, match="No fallback chain"):
        fallback_chain()


def test_fallback_chain_returns_the_registered_chain():
    set_fallback_chain(lambda _: ("en", "en-US"))

    assert fallback_chain() == ("en", "en-US")


def test_fallback_chain_passes_the_explicit_locale_through():
    called_with: list[str | None] = []

    def capture(locale: str | None) -> tuple[str, ...]:
        called_with.append(locale)
        return (locale or "en",)

    set_fallback_chain(capture)

    fallback_chain("de")

    assert called_with == ["de"]


def test_fallback_chain_passes_none_when_no_locale_is_given():
    called_with: list[str | None] = []

    def capture(locale: str | None) -> tuple[str, ...]:
        called_with.append(locale)
        return ("en",)

    set_fallback_chain(capture)

    fallback_chain()

    assert called_with == [None]


def test_fallback_chain_replacing_is_honoured():
    set_fallback_chain(lambda _: ("en",))
    set_fallback_chain(lambda _: ("de", "en"))

    assert fallback_chain() == ("de", "en")


def test_fallback_chain_slot_is_cleared_between_tests():
    assert _trans._get_fallback is None

    set_fallback_chain(lambda _: ("fr", "en"))

    assert fallback_chain() == ("fr", "en")


# --- select_current ---


@pytest.mark.parametrize(
    ("active", "data", "expected"),
    [
        ("en", {"en": "Hello", "pl": "Czesc"}, "Hello"),
        ("pl", {"en": "Hello", "pl": "Czesc"}, "Czesc"),
        # "de" is missing, so its chain walks down to "en"
        ("de", {"en": "Hello", "pl": "Czesc"}, "Hello"),
        # nothing in the chain matches: the first known translation is taken
        ("en", {"pl": "Czesc"}, "Czesc"),
    ],
)
@pytest.mark.usefixtures("locales")
def test_select_current_walks_the_chain(active, data, expected):
    locale.set(active)

    assert select_current(data) == expected


@pytest.mark.usefixtures("locales")
def test_select_current_honours_an_explicit_locale():
    locale.set("en")

    assert select_current({"en": "Hello", "pl": "Czesc"}, "pl") == "Czesc"


@pytest.mark.usefixtures("locales")
def test_select_current_of_an_empty_mapping_is_none():
    assert select_current({}) is None


# --- Translation ---


@pytest.mark.usefixtures("locales")
def test_translation_is_the_active_locale_text():
    translation = Translation("Hello", {"en": "Hello", "pl": "Czesc"})

    assert translation == "Hello"
    assert isinstance(translation, str)


@pytest.mark.usefixtures("locales")
def test_translation_without_data_keys_the_active_locale():
    locale.set("pl")

    assert Translation("Czesc").data == {"pl": "Czesc"}


@pytest.mark.usefixtures("locales")
def test_translation_data_is_a_copy():
    translation = Translation("Hello", {"en": "Hello"})

    translation.data["pl"] = "Czesc"

    assert translation.data == {"en": "Hello"}


@pytest.mark.usefixtures("locales")
def test_translation_get_returns_none_for_an_unknown_locale():
    translation = Translation("Hello", {"en": "Hello"})

    assert translation.get("en") == "Hello"
    assert translation.get("pl") is None


@pytest.mark.usefixtures("locales")
def test_translation_update_returns_a_new_instance():
    original = Translation("Hello", {"en": "Hello"})

    updated = original.update("Czesc", "pl")

    assert updated.data == {"en": "Hello", "pl": "Czesc"}
    assert original.data == {"en": "Hello"}


@pytest.mark.usefixtures("locales")
def test_translation_update_defaults_to_the_active_locale():
    locale.set("pl")

    updated = Translation("Hello", {"en": "Hello"}).update("Czesc")

    assert updated.data == {"en": "Hello", "pl": "Czesc"}
    assert updated == "Czesc"


@pytest.mark.usefixtures("locales")
def test_translation_update_of_an_unreachable_locale_falls_back_to_the_value():
    """No chain reaches "es", so the new text is what the copy reads as."""
    updated = Translation("Hello", {}).update("Hola", "es")

    assert updated == "Hola"


@pytest.mark.usefixtures("locales")
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Hello", {"en": "Hello"}),
        ({"en": "Hello", "pl": "Czesc"}, {"en": "Hello", "pl": "Czesc"}),
    ],
)
def test_translation_validate_accepts_text_and_mappings(value, expected):
    assert Translation._validate(value).data == expected


@pytest.mark.usefixtures("locales")
def test_translation_validate_rejects_none():
    with pytest.raises(ValueError, match="string or a mapping"):
        Translation._validate(None)


# --- pydantic integration ---


@pytest.mark.usefixtures("locales")
def test_translation_validates_from_a_string():
    result = TypeAdapter(Translation).validate_python("Hello")

    assert isinstance(result, Translation)
    assert result.data == {"en": "Hello"}


@pytest.mark.usefixtures("locales")
def test_translation_validates_from_a_mapping():
    locale.set("pl")

    result = TypeAdapter(Translation).validate_python({"en": "Hello", "pl": "Czesc"})

    assert result == "Czesc"
    assert result.data == {"en": "Hello", "pl": "Czesc"}


@pytest.mark.usefixtures("locales")
def test_translation_rejects_an_unusable_type_as_a_validation_error():
    """A `ValueError` is what pydantic turns into a validation error."""

    class Model(BaseModel):
        title: Translation

    with pytest.raises(ValueError, match="Cannot build a Translation from int"):
        Model(title=1)


@pytest.mark.usefixtures("locales")
def test_translation_serializes_to_the_active_locale_text():
    locale.set("pl")
    translation = Translation("Czesc", {"en": "Hello", "pl": "Czesc"})

    adapter = TypeAdapter(Translation)

    assert adapter.dump_python(translation) == "Czesc"
    assert adapter.dump_json(translation) == b'"Czesc"'


def test_translation_json_schema_in_validation_mode_accepts_both_shapes():
    schema = TypeAdapter(Translation).json_schema()

    assert "anyOf" in schema
    assert {"type": "string"} in schema["anyOf"]


def test_translation_json_schema_in_serialization_mode_is_a_string():
    schema = TypeAdapter(Translation).json_schema(mode="serialization")

    assert schema == {"type": "string"}


# --- as_translation ---


@pytest.mark.usefixtures("locales")
def test_as_translation_passes_none_through():
    assert as_translation(None) is None


@pytest.mark.usefixtures("locales")
def test_as_translation_passes_a_translation_through_unchanged():
    translation = Translation("Hello", {"en": "Hello"})

    assert as_translation(translation) is translation


@pytest.mark.usefixtures("locales")
def test_as_translation_keys_a_string_by_the_active_locale():
    locale.set("pl")

    assert as_translation("Czesc").data == {"pl": "Czesc"}


@pytest.mark.usefixtures("locales")
def test_as_translation_honours_an_explicit_locale():
    assert as_translation("Czesc", "pl").data == {"pl": "Czesc"}


@pytest.mark.usefixtures("locales")
def test_as_translation_of_a_mapping_selects_the_current_text():
    locale.set("pl")

    translation = as_translation({"en": "Hello", "pl": "Czesc"})

    assert translation == "Czesc"


@pytest.mark.usefixtures("locales")
def test_as_translation_of_an_empty_mapping_is_empty_text():
    assert as_translation({}) == ""


@pytest.mark.usefixtures("locales")
def test_as_translation_stringifies_mapping_keys():
    assert as_translation({1: "One"}).data == {"1": "One"}


@pytest.mark.usefixtures("locales")
def test_as_translation_rejects_a_non_string_text():
    with pytest.raises(ValueError, match="locale 'en' must be a string, got int"):
        as_translation({"en": 1})


@pytest.mark.usefixtures("locales")
def test_as_translation_rejects_an_unsupported_type():
    with pytest.raises(ValueError, match="Cannot build a Translation from float"):
        as_translation(1.5)


# --- TranslationMixin ---


@pytest.mark.usefixtures("locales")
def test_get_translations_returns_every_locale():
    post = Post(title={"en": "Hello", "pl": "Czesc"})

    assert post.get_translations("title") == {"en": "Hello", "pl": "Czesc"}


@pytest.mark.usefixtures("locales")
def test_get_translations_of_an_unset_field_is_empty():
    assert Post().get_translations("title") == {}


@pytest.mark.usefixtures("locales")
def test_get_translation_of_the_active_locale_walks_the_chain():
    locale.set("de")
    post = Post(title={"en": "Hello"})

    assert post.get_translation("title") == "Hello"


@pytest.mark.usefixtures("locales")
def test_get_translation_of_an_explicit_locale_does_not_walk_the_chain():
    """An explicit locale is looked up as given -- "de" is simply missing."""
    post = Post(title={"en": "Hello"})

    assert post.get_translation("title", "de") is None


@pytest.mark.usefixtures("locales")
def test_set_translation_merges_into_the_existing_locales():
    post = Post(title={"en": "Hello"})

    post.set_translation("title", "Czesc", "pl")

    assert post.get_translations("title") == {"en": "Hello", "pl": "Czesc"}


@pytest.mark.usefixtures("locales")
def test_set_translation_defaults_to_the_active_locale():
    locale.set("pl")
    post = Post(title={"en": "Hello"})

    post.set_translation("title", "Czesc")

    assert post.get_translation("title", "pl") == "Czesc"


@pytest.mark.usefixtures("locales")
def test_clear_translations_leaves_an_empty_translation():
    """The column keeps holding a translation, so it may be non-nullable."""
    post = Post(title={"en": "Hello"})

    post.clear_translations("title")

    assert post.title == ""
    assert post.get_translations("title") == {}


# --- TranslatedString ---


@pytest.mark.usefixtures("locales")
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("Hello", {"en": "Hello"}),
        ({"en": "Hello", "pl": "Czesc"}, {"en": "Hello", "pl": "Czesc"}),
    ],
)
def test_translated_string_binds_the_whole_locale_map(value, expected):
    assert TranslatedString().process_bind_param(value, sqlite.dialect()) == expected


@pytest.mark.usefixtures("locales")
def test_translated_string_reads_back_as_a_translation():
    result = TranslatedString().process_result_value(
        {"en": "Hello", "pl": "Czesc"}, sqlite.dialect()
    )

    assert isinstance(result, Translation)
    assert result == "Hello"
    assert result.data == {"en": "Hello", "pl": "Czesc"}


def test_translated_string_reads_none_as_none():
    assert TranslatedString().process_result_value(None, sqlite.dialect()) is None


@pytest.mark.usefixtures("locales")
def test_translated_string_skips_non_string_values_on_read():
    result = TranslatedString().process_result_value(
        {"en": "Hello", "count": 3}, sqlite.dialect()
    )

    assert result.data == {"en": "Hello"}


@pytest.mark.usefixtures("locales")
def test_translated_string_compares_whole_locale_maps():
    """Two values reading the same in the active locale still differ."""
    kind = TranslatedString()
    one = {"en": "Hello", "pl": "Czesc"}
    other = {"en": "Hello"}

    assert kind.compare_values(one, dict(one))
    assert not kind.compare_values(one, other)


@pytest.mark.usefixtures("locales")
def test_translated_string_compares_none():
    assert TranslatedString().compare_values(None, None)


@pytest.mark.usefixtures("locales")
def test_translated_string_filter_targets_the_active_locale_on_postgresql():
    sql = _compile(sa.select(Post).where(Post.title == "Hello"), postgresql.dialect())

    assert "->>" in sql


@pytest.mark.usefixtures("locales")
def test_translated_string_filter_falls_back_to_json_extract():
    sql = _compile(sa.select(Post).where(Post.title == "Hello"), sqlite.dialect())

    assert "json_extract(test_i18n_post.title, '$.\"en\"')" in sql


@pytest.mark.usefixtures("locales")
def test_translated_string_filter_unquotes_on_mysql():
    """``JSON_EXTRACT`` yields a quoted scalar there, which ``LIKE`` would
    match against including the quotes.
    """
    sql = _compile(sa.select(Post).where(Post.title.like("Hello%")), mysql.dialect())

    assert "json_unquote(json_extract(test_i18n_post.title, '$.\"en\"'))" in sql


@pytest.mark.usefixtures("locales")
def test_translated_string_ordering_targets_the_active_locale():
    ascending = _compile(sa.select(Post).order_by(Post.title.asc()), sqlite.dialect())
    descending = _compile(sa.select(Post).order_by(Post.title.desc()), sqlite.dialect())
    ordered = "json_extract(test_i18n_post.title, '$.\"en\"')"

    assert ascending.endswith(f"ORDER BY {ordered} ASC")
    assert descending.endswith(f"ORDER BY {ordered} DESC")


@pytest.mark.usefixtures("locales")
def test_translated_string_reverse_operations_target_the_active_locale():
    """Taken off the table: an ORM attribute routes its reflected operators
    through SQLAlchemy's own ``ColumnProperty.Comparator`` instead.
    """
    column = Post.__table__.c.title

    sql = _compile(sa.select("greeting" + column), sqlite.dialect())

    assert sql.startswith(
        "SELECT 'greeting' || json_extract(test_i18n_post.title, '$.\"en\"')"
    )


@pytest.mark.usefixtures("locales")
def test_translated_string_json_operators_still_address_the_locale_map():
    """The inherited JSON methods key by locale name, not by locale text."""
    sql = _compile(sa.select(Post).where(Post.title.has_key("pl")), sqlite.dialect())

    assert '$."pl"' in sql
    assert '$."en"' not in sql


async def test_translated_string_round_trips_through_the_database(posts, locales):
    await posts.create(title={"en": "Hello", "pl": "Czesc"})

    locales("pl")
    stored = await posts.select().one()

    assert stored.title == "Czesc"
    assert stored.title.data == {"en": "Hello", "pl": "Czesc"}


async def test_translated_string_filters_by_the_active_locale(posts, locales):
    await posts.create(title={"en": "Hello", "pl": "Czesc"})
    await posts.create(title={"en": "World", "pl": "Swiat"})

    locales("pl")
    matched = await posts.select().filter(Post.title == "Czesc").all()

    assert [row.title for row in matched] == ["Czesc"]
    assert await posts.count(Post.title == "Hello") == 0


async def test_translated_string_orders_by_the_active_locale(posts, locales):
    await posts.create(title={"en": "Zulu", "pl": "Alfa"})
    await posts.create(title={"en": "Alpha", "pl": "Zeta"})

    locales("pl")
    ordered = await posts.select().order_by(Post.title.asc()).all()

    assert [str(row.title) for row in ordered] == ["Alfa", "Zeta"]


@pytest.mark.usefixtures("locales")
async def test_translated_string_matches_with_like(posts):
    await posts.create(title={"en": "Hello world"})

    matched = await posts.select().filter(Post.title.like("Hello%")).all()

    assert len(matched) == 1


# --- translation_table ---


def test_translation_table_keys_on_the_parent_key_and_the_locale():
    table = ArticleTranslation.__table__

    assert set(table.primary_key.columns.keys()) == {"id", "locale"}


def test_translation_table_cascades_from_the_parent():
    (foreign_key,) = ArticleTranslation.__table__.c.id.foreign_keys

    assert foreign_key.column is Article.__table__.c.id
    assert foreign_key.ondelete == "CASCADE"


def test_translation_table_sizes_the_locale_column():
    assert ArticleTranslation.__table__.c.locale.type.length == LOCALE_LENGTH


def test_translation_table_makes_translated_columns_nullable():
    """Declared NOT NULL, but a locale row holds only what it translates."""
    assert ArticleTranslation.__table__.c.title.nullable
    assert ArticleTranslation.__table__.c.body.nullable


def test_translation_table_rejects_a_field_it_has_no_column_for(local_base):
    class Doc(TranslatableMixin, local_base):
        __tablename__ = "doc"
        __translated_fields__ = ("headline",)

        id = sa.Column(sa.Integer, primary_key=True)

    with pytest.raises(TypeError, match="has no column 'headline'"):

        class DocTranslation(translation_table(Doc)):
            __tablename__ = "doc_translation"

            body = sa.Column(sa.Unicode(50))


def test_translation_table_rejects_a_non_declarative_parent():
    with pytest.raises(TypeError, match="is not a declarative model"):
        _declarative_base(object)


def test_translation_class_resolves_the_registered_model():
    assert translation_class(Article) is ArticleTranslation


def test_translation_class_resolves_through_the_mro(local_base):
    class Doc(TranslatableMixin, local_base):
        __tablename__ = "doc"
        __translated_fields__ = ("title",)

        id = sa.Column(sa.Integer, primary_key=True)

    class DocTranslation(translation_table(Doc)):
        __tablename__ = "doc_translation"

        title = sa.Column(sa.Unicode(50))

    class Guide(Doc):
        """A subclass inherits the translation table its parent declared."""

    assert translation_class(Guide) is DocTranslation


def test_translation_class_raises_for_a_model_without_one():
    with pytest.raises(LookupError, match="No translation table declared"):
        translation_class(Untranslated)


def test_current_translation_is_the_join_target_relationship():
    assert current_translation(Article) is Article._current_translation


# --- TranslatableMixin ---


@pytest.mark.usefixtures("locales")
async def test_translatable_writes_and_reads_every_locale(articles):
    async with articles.session() as session:
        article = Article()
        article.title = {"en": "Hello", "pl": "Czesc"}
        session.add(article)
        await session.flush()

        assert article.get_translations("title") == {"en": "Hello", "pl": "Czesc"}
        locale.set("pl")
        assert article.title == "Czesc"


@pytest.mark.usefixtures("locales")
async def test_translatable_leaves_an_untranslated_field_null(articles):
    async with articles.session() as session:
        article = Article()
        article.title = {"en": "Hello", "pl": "Czesc"}
        article.body = {"en": "Body"}
        session.add(article)
        await session.flush()

        assert article._translations["pl"].body is None
        assert article.get_translations("body") == {"en": "Body"}


@pytest.mark.usefixtures("locales")
async def test_translatable_assigning_a_string_only_touches_the_active_locale(articles):
    async with articles.session() as session:
        article = Article()
        article.title = {"en": "Hello", "pl": "Czesc"}
        session.add(article)
        await session.flush()

        locale.set("pl")
        article.title = "Witaj"

        assert article.get_translations("title") == {"en": "Hello", "pl": "Witaj"}


@pytest.mark.usefixtures("locales")
async def test_translatable_assigning_none_clears_every_locale(articles):
    async with articles.session() as session:
        article = Article()
        article.title = {"en": "Hello", "pl": "Czesc"}
        session.add(article)
        await session.flush()

        article.title = None

        assert article.get_translations("title") == {}
        assert article.title is None


@pytest.mark.usefixtures("locales")
async def test_translatable_falls_back_across_locales(articles):
    async with articles.session() as session:
        article = Article()
        article.title = {"en": "Hello"}
        session.add(article)
        await session.flush()

        locale.set("de")

        assert article.title == "Hello"


@pytest.mark.usefixtures("locales")
def test_translatable_delegates_an_untranslated_field_to_the_mixin():
    article = Article(slug="hello")

    assert article.get_translations("slug") == {"en": "hello"}


# --- TranslatedRepository ---


def test_translated_repository_resolves_the_model_from_the_subscript():
    assert ArticleRepository.model is Article


def test_translated_repository_select_includes_the_outer_join():
    sql = str(ArticleRepository().select().query)

    assert "LEFT OUTER JOIN" in sql
    assert ArticleTranslation.__tablename__ in sql


def test_translated_repository_select_accepts_column_args():
    sql = str(ArticleRepository().select(Article.id).query)

    assert "LEFT OUTER JOIN" in sql
    assert Article.__tablename__ in sql


def test_translated_repository_rejects_a_model_without_the_mixin():
    with pytest.raises(TypeError, match="must inherit from TranslatableMixin"):

        class Repository(TranslatedRepository[Untranslated]):
            pass


def test_translated_repository_allows_an_abstract_subclass():
    class Abstract(TranslatedRepository, abstract=True):
        """An abstract subclass declares no model of its own."""

    assert not hasattr(Abstract, "model")


@pytest.mark.usefixtures("locales")
async def test_translated_repository_filters_by_a_translated_field(articles):
    async with articles.session() as session:
        for titles in ({"en": "Hello", "pl": "Czesc"}, {"en": "World", "pl": "Swiat"}):
            article = Article()
            article.title = titles
            session.add(article)
        await session.flush()

    locale.set("pl")
    matched = await articles.select().filter(Article.title == "Czesc").all()

    assert len(matched) == 1
    assert matched[0].title == "Czesc"


@pytest.mark.usefixtures("locales")
async def test_translated_repository_orders_by_a_translated_field(articles):
    async with articles.session() as session:
        for titles in ({"en": "Zulu", "pl": "Alfa"}, {"en": "Alpha", "pl": "Zeta"}):
            article = Article()
            article.title = titles
            session.add(article)
        await session.flush()

    locale.set("pl")
    ordered = await articles.select().order_by(Article.title.asc()).all()

    assert [str(row.title) for row in ordered] == ["Alfa", "Zeta"]
