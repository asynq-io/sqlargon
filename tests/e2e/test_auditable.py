"""The append-only repository against a real server.

What only a real backend can prove: the correlated subquery scoping every
read, the ``INSERT ... SELECT`` an append compiles to, the composite foreign
key pinning a child to one version, and -- on MySQL, which has no RETURNING
at all -- the path that has to find the appended rows again.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from sqlargon import AppendOnlyError, ConcurrentModificationError

from .models import AuditArticle, AuditComment, AuditFollow, UUIDAuditArticle


@pytest.fixture
async def article(audit_articles):
    """One article carried up to version 3."""
    created = await audit_articles.create(name="draft", tag="news")
    await audit_articles.update_one({"name": "revised"}, AuditArticle.id == created.id)
    await audit_articles.update_one({"name": "final"}, AuditArticle.id == created.id)
    return created.id


# --- appending ---


async def test_update_appends_a_row_instead_of_rewriting_one(
    article, raw_audit_articles
):
    stored = await raw_audit_articles.list(AuditArticle.id == article)

    assert [(row.version, row.name) for row in stored] == [
        (1, "draft"),
        (2, "revised"),
        (3, "final"),
    ]
    # a column the caller never named rides along
    assert {row.tag for row in stored} == {"news"}


async def test_reads_are_scoped_to_the_newest_version(article, audit_articles):
    head = await audit_articles.get(id=article)

    assert (head.version, head.name) == (3, "final")
    assert await audit_articles.count() == 1
    assert await audit_articles.versions().count() == 3


async def test_update_many_appends_one_version_per_entity(
    audit_articles, raw_audit_articles
):
    first = await audit_articles.create(name="a")
    second = await audit_articles.create(name="b")

    appended = await audit_articles.update_many(
        {"tag": "shared"}, AuditArticle.id.in_([first.id, second.id])
    )

    assert {row.version for row in appended} == {2}
    assert {row.tag for row in appended} == {"shared"}
    assert len(await raw_audit_articles.list()) == 4


async def test_the_fluent_builder_appends_in_one_statement(
    article, audit_articles, raw_audit_articles
):
    await (
        audit_articles.update({"name": "fluent"})
        .filter(AuditArticle.id == article)
        .execute()
    )

    assert (await audit_articles.get(id=article)).name == "fluent"
    assert len(await raw_audit_articles.list(AuditArticle.id == article)) == 4


async def test_create_or_update_appends_or_creates(article, audit_articles):
    appended = await audit_articles.create_or_update(id=article, name="fourth")
    created = await audit_articles.create_or_update(id=uuid4(), name="fresh")

    assert (appended.version, appended.name) == (4, "fourth")
    assert created.version == 1


async def test_bulk_create_or_update_appends_and_creates(article, audit_articles):
    fresh = uuid4()

    await audit_articles.bulk_create_or_update(
        [{"id": article, "name": "appended"}, {"id": fresh, "name": "created"}]
    )

    assert (await audit_articles.get(id=article)).version == 4
    assert (await audit_articles.get(id=fresh)).version == 1


async def test_bulk_update_appends_one_version_per_row(audit_articles):
    first = await audit_articles.create(name="a")
    second = await audit_articles.create(name="b")

    await audit_articles.bulk_update(
        [{"id": first.id, "name": "a2"}, {"id": second.id, "name": "b2"}]
    )

    assert (await audit_articles.get(id=first.id)).name == "a2"
    assert (await audit_articles.get(id=second.id)).name == "b2"


async def test_upsert_is_refused(audit_articles):
    with pytest.raises(AppendOnlyError, match="cannot resolve a conflict"):
        audit_articles.upsert([{"id": uuid4(), "name": "x"}])


# --- deletion is an appended tombstone ---


async def test_remove_appends_a_tombstoned_version(
    article, audit_articles, raw_audit_articles
):
    await audit_articles.remove(AuditArticle.id == article)

    stored = await raw_audit_articles.list(AuditArticle.id == article)
    assert [(row.version, row.tombstone) for row in stored[-1:]] == [(4, True)]
    assert await audit_articles.get(id=article) is None
    assert await audit_articles.versions().count() == 4


async def test_restore_appends_a_live_version(article, audit_articles):
    await audit_articles.remove(AuditArticle.id == article)

    restored = await audit_articles.restore(AuditArticle.id == article)

    assert [(row.version, row.tombstone) for row in restored] == [(5, False)]
    assert (await audit_articles.get(id=article)).name == "final"


# --- inspecting the history ---


async def test_history_returns_every_version_oldest_first(article, audit_articles):
    history = await audit_articles.history(id=article)

    assert [(row.version, row.name) for row in history] == [
        (1, "draft"),
        (2, "revised"),
        (3, "final"),
    ]


async def test_get_version_returns_one_exact_version(article, audit_articles):
    assert (await audit_articles.get_version(2, id=article)).name == "revised"
    assert await audit_articles.get_version(9, id=article) is None


async def test_at_reads_the_state_of_that_moment(
    article, audit_articles, raw_audit_articles
):
    second = (await raw_audit_articles.list(AuditArticle.id == article))[1]

    as_of = await audit_articles.at(second.created_at).get(id=article)

    assert (as_of.version, as_of.name) == (2, "revised")


async def test_at_hides_an_entity_already_deleted_by_then(
    article, audit_articles, raw_audit_articles
):
    await audit_articles.remove(AuditArticle.id == article)
    tombstone = (await raw_audit_articles.list(AuditArticle.id == article))[-1]

    assert await audit_articles.at(tombstone.created_at).get(id=article) is None


# --- optimistic concurrency ---


async def test_update_if_match_appends_when_the_version_is_current(
    article, audit_articles
):
    appended = await audit_articles.update_if_match(
        {"name": "guarded"}, AuditArticle.id == article, expected_version=3
    )

    assert (appended.version, appended.name) == (4, "guarded")


async def test_update_if_match_refuses_a_stale_version(article, audit_articles):
    with pytest.raises(ConcurrentModificationError):
        await audit_articles.update_if_match(
            {"name": "stale"},
            AuditArticle.id == article,
            expected_version=1,
            raise_on_mismatch=True,
        )


async def test_a_duplicate_version_collides_on_the_primary_key(
    article, raw_audit_articles
):
    with pytest.raises(IntegrityError):
        await raw_audit_articles.insert(
            [{"id": article, "version": 3, "name": "racing"}]
        ).execute()


# --- relationships ---


async def test_pinned_relationship_stays_on_its_version(
    article, audit_articles, audit_comments
):
    await audit_comments.create(article_id=article, article_version=2, body="on v2")
    await audit_articles.update_one({"name": "later"}, AuditArticle.id == article)

    comment = await audit_comments.load(AuditComment.article).one()

    assert (comment.article.version, comment.article.name) == (2, "revised")


async def test_latest_relationship_follows_the_entity_forward(
    article, audit_articles, audit_follows
):
    await audit_follows.create(article_id=article)

    before = await audit_follows.load(AuditFollow.article).one()
    assert (before.article.version, before.article.name) == (3, "final")

    await audit_articles.update_one({"name": "newest"}, AuditArticle.id == article)
    after = await audit_follows.load(AuditFollow.article).one()

    assert (after.article.version, after.article.name) == (4, "newest")


async def test_latest_relationship_joins_in_sql(article, audit_follows):
    await audit_follows.create(article_id=article)

    names = (
        await audit_follows.select(AuditArticle.name).join(AuditFollow.article).all()
    )

    assert names == ["final"]


# --- purging superseded versions ---


async def test_purge_keeps_the_newest_version_only(
    article, audit_articles, raw_audit_articles
):
    await audit_articles.purge(id=article)

    stored = await raw_audit_articles.list(AuditArticle.id == article)
    assert [(row.version, row.name) for row in stored] == [(3, "final")]


async def test_purge_leaves_other_entities_alone(
    article, audit_articles, raw_audit_articles
):
    other = await audit_articles.create(name="other")
    await audit_articles.update_one({"name": "other2"}, AuditArticle.id == other.id)

    await audit_articles.purge(id=article)

    assert len(await raw_audit_articles.list(AuditArticle.id == other.id)) == 2


@pytest.mark.usefixtures("needs_foreign_keys")
async def test_a_pinned_version_cannot_be_purged(
    article, audit_articles, audit_comments
):
    await audit_comments.create(article_id=article, article_version=1, body="pinned")

    with pytest.raises(IntegrityError):
        await audit_articles.purge(id=article)


# --- the UUIDv7 strategy ---


async def test_uuid_strategy_appends_sortable_versions(uuid_audit_articles):
    entity_id = uuid4()

    await uuid_audit_articles.create(id=entity_id, name="draft")
    await uuid_audit_articles.update_one(
        {"name": "final"}, UUIDAuditArticle.id == entity_id
    )

    history = await uuid_audit_articles.history(id=entity_id)
    versions = [row.version for row in history]

    assert [row.name for row in history] == ["draft", "final"]
    assert versions == sorted(versions)
    assert (await uuid_audit_articles.get(id=entity_id)).name == "final"


async def test_uuid_strategy_deletes_by_appending_a_tombstone(uuid_audit_articles):
    entity_id = uuid4()
    await uuid_audit_articles.create(id=entity_id, name="draft")

    await uuid_audit_articles.remove(UUIDAuditArticle.id == entity_id)

    assert await uuid_audit_articles.get(id=entity_id) is None
    assert len(await uuid_audit_articles.history(id=entity_id)) == 2


# --- the scope reaches the server, not just the ORM ---


async def test_the_latest_scope_is_one_sql_statement(article, audit_articles):
    statement = audit_articles.select().filter(AuditArticle.id == article).query
    compiled = str(statement.compile())

    assert "ORDER BY" in compiled
    assert compiled.count("e2e_audit_article") >= 2


async def test_count_of_a_history_spanning_two_entities(audit_articles):
    first = await audit_articles.create(name="a")
    await audit_articles.create(name="b")
    await audit_articles.update_one({"name": "a2"}, AuditArticle.id == first.id)

    assert await audit_articles.count() == 2
    assert await audit_articles.versions().count() == 3
    assert await audit_articles.count(sa.true()) == 2
