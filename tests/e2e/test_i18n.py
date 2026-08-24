"""E2E tests for both i18n backends across real database backends.

The JSON column backend reads a locale out of a document, which each dialect
spells its own way -- the point of running it here rather than on SQLite alone.
"""

from __future__ import annotations

import pytest

from .models import I18nArticle, I18nPost

TITLES = {"en": "Hello", "pl": "Czesc"}


async def seed_posts(i18n_posts, *documents: dict[str, str]) -> None:
    for document in documents:
        await i18n_posts.create(title=document)


async def seed_articles(i18n_articles, *documents: dict[str, str]) -> None:
    async with i18n_articles.session() as session:
        for document in documents:
            article = I18nArticle()
            article.title = document
            session.add(article)
        await session.flush()


# --- the JSON column backend ---


@pytest.mark.usefixtures("locales")
async def test_json_backend_round_trips_every_locale(i18n_posts):
    await seed_posts(i18n_posts, TITLES)

    stored = await i18n_posts.select().one()

    assert stored.title.data == TITLES


async def test_json_backend_reads_the_active_locale(i18n_posts, locales):
    await seed_posts(i18n_posts, TITLES)

    locales("pl")
    assert str((await i18n_posts.select().one()).title) == "Czesc"

    locales("en")
    assert str((await i18n_posts.select().one()).title) == "Hello"


async def test_json_backend_falls_back_across_locales(i18n_posts, locales):
    await seed_posts(i18n_posts, {"en": "Hello"})

    locales("de")

    assert str((await i18n_posts.select().one()).title) == "Hello"


async def test_json_backend_filters_by_the_active_locale(i18n_posts, locales):
    await seed_posts(i18n_posts, TITLES, {"en": "World", "pl": "Swiat"})

    locales("pl")

    assert await i18n_posts.count(I18nPost.title == "Czesc") == 1
    assert await i18n_posts.count(I18nPost.title == "Hello") == 0


async def test_json_backend_matches_with_like(i18n_posts, locales):
    await seed_posts(i18n_posts, {"en": "Hello world", "pl": "Czesc swiecie"})

    locales("pl")

    assert await i18n_posts.count(I18nPost.title.like("Czesc%")) == 1
    assert await i18n_posts.count(I18nPost.title.like("Hello%")) == 0


async def test_json_backend_orders_by_the_active_locale(i18n_posts, locales):
    await seed_posts(
        i18n_posts, {"en": "Zulu", "pl": "Alfa"}, {"en": "Alpha", "pl": "Zeta"}
    )

    locales("pl")
    ordered = await i18n_posts.select().order_by(I18nPost.title.asc()).all()

    assert [str(row.title) for row in ordered] == ["Alfa", "Zeta"]


# --- the translation table backend ---


@pytest.mark.usefixtures("locales")
async def test_translation_table_round_trips_every_locale(i18n_articles):
    await seed_articles(i18n_articles, TITLES)

    stored = await i18n_articles.select().one()

    assert stored.get_translations("title") == TITLES


async def test_translation_table_reads_the_active_locale(i18n_articles, locales):
    await seed_articles(i18n_articles, TITLES)

    locales("pl")
    assert str((await i18n_articles.select().one()).title) == "Czesc"

    locales("en")
    assert str((await i18n_articles.select().one()).title) == "Hello"


async def test_translation_table_leaves_an_untranslated_field_null(
    i18n_articles, locales
):
    async with i18n_articles.session() as session:
        article = I18nArticle()
        article.title = TITLES
        article.body = {"en": "Body"}
        session.add(article)
        await session.flush()

    locales("en")
    stored = await i18n_articles.select().one()

    assert stored.get_translations("body") == {"en": "Body"}
    assert stored.get_translations("title") == TITLES


async def test_translation_table_filters_by_a_translated_field(i18n_articles, locales):
    await seed_articles(i18n_articles, TITLES, {"en": "World", "pl": "Swiat"})

    locales("pl")
    matched = await i18n_articles.select().filter(I18nArticle.title == "Czesc").all()

    assert [str(row.title) for row in matched] == ["Czesc"]


async def test_translation_table_orders_by_a_translated_field(i18n_articles, locales):
    await seed_articles(
        i18n_articles, {"en": "Zulu", "pl": "Alfa"}, {"en": "Alpha", "pl": "Zeta"}
    )

    locales("pl")
    ordered = await i18n_articles.select().order_by(I18nArticle.title.asc()).all()

    assert [str(row.title) for row in ordered] == ["Alfa", "Zeta"]


@pytest.mark.usefixtures("needs_foreign_keys")
async def test_translation_table_cascades_from_the_parent(i18n_articles, locales):
    await seed_articles(i18n_articles, TITLES)
    locales("en")
    article = await i18n_articles.select().one()

    await i18n_articles.delete_one(I18nArticle.id == article.id)

    assert await i18n_articles.count() == 0
