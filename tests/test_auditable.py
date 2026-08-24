from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, mapped_column, relationship

from sqlargon import (
    AppendOnlyError,
    AuditableBase,
    AuditableRepository,
    Base,
    ConcurrentModificationError,
    Database,
    SQLAlchemyRepository,
    UUIDAuditableBase,
    latest_relationship,
    version_foreign_key,
    version_mapped_column,
)
from sqlargon.dialects.sqlite import SQLiteQueryBuilder
from sqlargon.mixins import AuditableMixin, IntegerAuditableMixin, UUIDModelMixin
from sqlargon.query_builder import Option
from sqlargon.types import GUID
from tests import MEMORY_URL

# Models defined at module level to avoid re-registration with --count=3


class Article(AuditableBase):
    __tablename__ = "test_auditable_article"
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Unicode(255), nullable=True)
    tag = sa.Column(sa.Unicode(255), nullable=True)


class UUIDArticle(UUIDModelMixin, UUIDAuditableBase):
    """The same shape, versioned by UUIDv7 rather than by a counter."""

    __tablename__ = "test_auditable_uuid_article"
    name: Mapped[str | None] = mapped_column(sa.Unicode(255), nullable=True)


class MixedIn(IntegerAuditableMixin, Base):
    """The mixin combined with ``Base`` by hand, rather than AuditableBase."""

    __tablename__ = "test_auditable_mixed_in"
    id = sa.Column(sa.Integer, primary_key=True)


class VersionOnly(AuditableBase):
    """Keyed by its version alone, so no entity can be told from another."""

    __tablename__ = "test_auditable_version_only"


class Plain(Base):
    __tablename__ = "test_auditable_plain"
    id = sa.Column(sa.Integer, primary_key=True)


class Comment(Base):
    """A child pinned to one exact version of an article."""

    __tablename__ = "test_auditable_comment"
    id = sa.Column(sa.Integer, primary_key=True)
    article_id = sa.Column(sa.Integer)
    article_version = version_mapped_column(Article)
    body = sa.Column(sa.Unicode(255), nullable=True)

    __table_args__ = (version_foreign_key(Article, "article_id", "article_version"),)

    article: Mapped[Article] = relationship()


class Follow(Base):
    """A child following whichever version of an article is newest."""

    __tablename__ = "test_auditable_follow"
    id = sa.Column(sa.Integer, primary_key=True)
    article_id = sa.Column(sa.Integer)

    article: Mapped[Article] = latest_relationship(Article, "article_id")


class ArticleRepository(AuditableRepository[Article]):
    pass


class RawArticleRepository(SQLAlchemyRepository[Article]):
    """Unscoped view of the same table, to observe what is physically stored."""

    default_order_by = Article.version


class UUIDArticleRepository(AuditableRepository[UUIDArticle]):
    pass


class CommentRepository(SQLAlchemyRepository[Comment]):
    pass


class FollowRepository(SQLAlchemyRepository[Follow]):
    pass


TABLES = (
    Article.__table__,
    UUIDArticle.__table__,
    Comment.__table__,
    Follow.__table__,
)


@pytest.fixture(autouse=True)
async def tables(db: Database):
    async with db.engine.begin() as conn:
        for table in TABLES:
            await conn.run_sync(table.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        for table in reversed(TABLES):
            await conn.run_sync(table.drop, checkfirst=True)


@pytest.fixture
def repository():
    return ArticleRepository()


@pytest.fixture
def raw():
    return RawArticleRepository()


@pytest.fixture
async def articles(repository):
    """A repository holding one article carried up to version 3."""
    await repository.create(id=1, name="draft", tag="news")
    await repository.update_one({"name": "revised"}, Article.id == 1)
    await repository.update_one({"name": "final"}, Article.id == 1)
    return repository


# --- model validation ---


def test_model_without_auditable_mixin_is_rejected():
    with pytest.raises(TypeError, match="Plain must inherit from AuditableMixin"):

        class BadRepository(AuditableRepository[Plain]):  # type: ignore[type-var]
            pass


def test_model_keyed_by_version_alone_is_rejected():
    with pytest.raises(TypeError, match="VersionOnly is keyed by its version alone"):

        class BadRepository(AuditableRepository[VersionOnly]):
            pass


def test_model_declaring_the_mixin_by_hand_is_accepted():
    # the static bound asks for AnyAuditableBase, but the version column and
    # its expressions are all the repository actually needs
    class MixedInRepository(AuditableRepository[MixedIn]):  # type: ignore[type-var]
        pass

    assert MixedInRepository.model is MixedIn


def test_abstract_subclass_needs_no_model():
    class Shared(AuditableRepository[Article], abstract=True):
        pass

    class Concrete(Shared):
        pass

    assert Concrete.model is Article


def test_model_can_be_passed_explicitly():
    class Explicit(AuditableRepository, model=Article):
        pass

    assert Explicit.model is Article


def test_version_joins_the_primary_key():
    assert {c.name for c in Article.__table__.primary_key.columns} == {"id", "version"}
    assert Article.audit_key() == ("id",)


def test_version_and_tombstone_are_left_out_of_the_conflict_set():
    default_set = ArticleRepository._get_default_set()
    assert "version" not in default_set
    assert "tombstone" not in default_set


# --- appending instead of updating ---


async def test_create_starts_at_version_one(repository, raw):
    created = await repository.create(id=1, name="draft")

    assert created.version == 1
    assert len(await raw.list()) == 1


@pytest.mark.usefixtures("articles")
async def test_update_appends_a_row_and_leaves_the_old_one_alone(raw):
    stored = await raw.list()

    assert [(row.id, row.version, row.name) for row in stored] == [
        (1, 1, "draft"),
        (1, 2, "revised"),
        (1, 3, "final"),
    ]


@pytest.mark.usefixtures("articles")
async def test_append_carries_columns_the_caller_did_not_name(raw):
    assert [row.tag for row in await raw.list()] == ["news", "news", "news"]


@pytest.mark.usefixtures("articles")
async def test_each_version_is_timestamped_on_its_own(raw):
    stored = await raw.list()
    timestamps = [row.created_at for row in stored]

    assert timestamps == sorted(timestamps)
    assert len(set(timestamps)) == len(timestamps)
    # nothing is ever updated, so the two timestamps never diverge
    assert all(row.created_at == row.updated_at for row in stored)


async def test_update_many_appends_one_version_per_entity(repository, raw):
    await repository.bulk_create([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    appended = await repository.update_many({"tag": "shared"}, Article.id.in_([1, 2]))

    assert {row.version for row in appended} == {2}
    assert len(await raw.list()) == 4


async def test_create_or_update_appends_the_next_version(articles):
    appended = await articles.create_or_update(id=1, name="fifth")

    assert appended.version == 4
    assert appended.name == "fifth"


async def test_create_or_update_creates_an_absent_entity(repository):
    created = await repository.create_or_update(id=7, name="fresh")

    assert (created.id, created.version) == (7, 1)


async def test_create_or_update_revives_a_deleted_entity(repository):
    await repository.create(id=1, name="draft")
    await repository.remove(Article.id == 1)

    revived = await repository.create_or_update(id=1, name="back")

    assert (revived.version, revived.tombstone) == (3, False)
    assert await repository.get(id=1) is not None


async def test_bulk_update_appends_one_version_per_row(repository, raw):
    await repository.bulk_create([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    await repository.bulk_update([{"id": 1, "name": "a2"}, {"id": 2, "name": "b2"}])

    assert [(row.id, row.version, row.name) for row in await raw.list()] == [
        (1, 1, "a"),
        (2, 1, "b"),
        (1, 2, "a2"),
        (2, 2, "b2"),
    ]


# --- reads see the newest version ---


@pytest.mark.parametrize("method", ["all", "list"])
async def test_reads_return_only_the_newest_version(articles, method):
    rows = await getattr(articles.select() if method == "all" else articles, method)()

    assert [(row.version, row.name) for row in rows] == [(3, "final")]


async def test_get_returns_the_newest_version(articles):
    assert (await articles.get(id=1)).name == "final"


async def test_count_counts_entities_while_versions_counts_rows(articles):
    assert await articles.count() == 1
    assert await articles.versions().count() == 3


async def test_selecting_columns_is_scoped_too(articles):
    assert await articles.select(Article.name).all() == ["final"]


async def test_reads_of_two_entities_pick_each_ones_newest(repository):
    await repository.bulk_create([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
    await repository.update_one({"name": "a2"}, Article.id == 1)

    rows = await repository.list()

    assert sorted((row.id, row.version, row.name) for row in rows) == [
        (1, 2, "a2"),
        (2, 1, "b"),
    ]


# --- delete appends a tombstone ---


@pytest.mark.parametrize("method", ["remove", "delete_one", "delete_many"])
async def test_delete_appends_a_tombstoned_version(articles, raw, method):
    await getattr(articles, method)(Article.id == 1)

    stored = await raw.list()
    assert [(row.version, row.tombstone) for row in stored[-1:]] == [(4, True)]
    assert len(stored) == 4


async def test_a_deleted_entity_leaves_reads_but_keeps_its_history(articles):
    await articles.remove(Article.id == 1)

    assert await articles.list() == []
    assert await articles.count() == 0
    assert await articles.versions().count() == 4


async def test_with_deleted_sees_the_tombstoned_head(articles):
    await articles.remove(Article.id == 1)

    head = await articles.with_deleted().get(id=1)

    assert (head.version, head.tombstone) == (4, True)


async def test_only_deleted_scopes_to_entities_whose_head_is_a_tombstone(articles):
    await articles.bulk_create([{"id": 2, "name": "live"}])
    await articles.remove(Article.id == 1)

    assert [row.id for row in await articles.only_deleted().list()] == [1]


async def test_restore_appends_a_live_version(articles, raw):
    await articles.remove(Article.id == 1)

    restored = await articles.restore(Article.id == 1)

    assert [(row.version, row.tombstone) for row in restored] == [(5, False)]
    assert (await articles.get(id=1)).name == "final"
    assert len(await raw.list()) == 5


# --- inspecting the history ---


async def test_history_returns_every_version_oldest_first(articles):
    assert [(row.version, row.name) for row in await articles.history(id=1)] == [
        (1, "draft"),
        (2, "revised"),
        (3, "final"),
    ]


async def test_history_covers_tombstoned_versions(articles):
    await articles.remove(Article.id == 1)

    assert [row.version for row in await articles.history(id=1)] == [1, 2, 3, 4]


async def test_get_version_returns_one_exact_version(articles):
    assert (await articles.get_version(2, id=1)).name == "revised"
    assert await articles.get_version(9, id=1) is None


async def test_versions_is_an_unscoped_view(articles):
    rows = await articles.versions().list()

    assert sorted(row.version for row in rows) == [1, 2, 3]


async def test_at_reads_the_state_of_that_moment(articles, raw):
    second = (await raw.list())[1]

    as_of = await articles.at(second.created_at).get(id=1)

    assert (as_of.version, as_of.name) == (2, "revised")


async def test_at_hides_an_entity_already_deleted_by_then(articles, raw):
    await articles.remove(Article.id == 1)
    tombstone = (await raw.list())[-1]

    assert await articles.at(tombstone.created_at).get(id=1) is None


async def test_at_predates_an_entity_that_did_not_exist_yet(articles, raw):
    first = (await raw.list())[0]
    await articles.create(id=2, name="later")

    assert [row.id for row in await articles.at(first.created_at).list()] == [1]


# --- the builders append rather than rewrite ---


async def test_update_builder_appends_in_one_statement(articles, raw):
    await articles.update({"name": "fluent"}).filter(Article.id == 1).execute()

    assert [(row.version, row.name) for row in await raw.list()][-1:] == [(4, "fluent")]


async def test_update_builder_is_awaitable_directly(articles):
    await articles.update({"name": "awaited"}).filter(Article.id == 1)

    assert (await articles.get(id=1)).name == "awaited"


async def test_update_builder_only_touches_matched_entities(repository, raw):
    await repository.bulk_create([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    await repository.update({"name": "only one"}).filter(Article.id == 1).execute()

    assert sorted((row.id, row.version) for row in await raw.list()) == [
        (1, 1),
        (1, 2),
        (2, 1),
    ]


async def test_update_builder_can_return_the_appended_rows(articles):
    appended = (
        await articles.update({"name": "returned"}, return_results=True)
        .filter(Article.id == 1)
        .all()
    )

    assert [(row.version, row.name) for row in appended] == [(4, "returned")]


async def test_update_builder_reads_the_scope_it_was_built_from(articles, raw):
    await articles.remove(Article.id == 1)

    # the entity is tombstoned, so the live scope matches nothing to append to
    await articles.update({"name": "ignored"}).filter(Article.id == 1).execute()

    assert len(await raw.list()) == 4


async def test_delete_builder_appends_a_tombstone(articles, raw):
    await articles.delete().filter(Article.id == 1).execute()

    assert [(row.version, row.tombstone) for row in await raw.list()][-1:] == [
        (4, True)
    ]


async def test_update_builder_accepts_a_sql_expression(articles):
    appended = await articles.update_one({"name": Article.tag}, Article.id == 1)

    assert appended.name == "news"


async def test_update_builder_rejects_many_value_sets(repository):
    with pytest.raises(AppendOnlyError, match="takes a single mapping"):
        repository.update([{"name": "a"}, {"name": "b"}])


def test_upsert_is_refused(repository):
    with pytest.raises(AppendOnlyError, match="cannot resolve a conflict"):
        repository.upsert([{"id": 1}])


# --- bulk create or update ---


async def test_bulk_create_or_update_appends_and_creates(articles, raw):
    await articles.bulk_create_or_update(
        [{"id": 1, "name": "appended"}, {"id": 9, "name": "created"}]
    )

    assert sorted((row.id, row.version, row.name) for row in await raw.list()) == [
        (1, 1, "draft"),
        (1, 2, "revised"),
        (1, 3, "final"),
        (1, 4, "appended"),
        (9, 1, "created"),
    ]


async def test_bulk_create_or_update_can_return_the_written_rows(articles):
    written = await articles.bulk_create_or_update(
        [{"id": 1, "name": "appended"}, {"id": 9, "name": "created"}],
        return_results=True,
    )

    assert sorted((row.id, row.version) for row in written) == [(1, 4), (9, 1)]


async def test_bulk_create_or_update_revives_a_deleted_entity(repository):
    await repository.create(id=1, name="draft")
    await repository.remove(Article.id == 1)

    await repository.bulk_create_or_update([{"id": 1, "name": "back"}])

    assert (await repository.get(id=1)).name == "back"


async def test_bulk_create_or_update_needs_the_entity_key(repository):
    with pytest.raises(AppendOnlyError, match="entity key"):
        await repository.bulk_create_or_update([{"name": "keyless"}])


async def test_bulk_create_or_update_of_nothing_is_a_no_op(repository, raw):
    await repository.bulk_create_or_update([])

    assert await raw.list() == []


# --- optimistic concurrency ---


async def test_update_if_match_appends_when_the_version_is_current(articles):
    appended = await articles.update_if_match(
        {"name": "guarded"}, Article.id == 1, expected_version=3
    )

    assert (appended.version, appended.name) == (4, "guarded")


async def test_update_if_match_refuses_a_stale_version(articles, raw):
    assert (
        await articles.update_if_match(
            {"name": "stale"}, Article.id == 1, expected_version=2
        )
        is None
    )
    assert len(await raw.list()) == 3


async def test_update_if_match_can_raise_on_a_stale_version(articles):
    with pytest.raises(ConcurrentModificationError, match="version 2"):
        await articles.update_if_match(
            {"name": "stale"},
            Article.id == 1,
            expected_version=2,
            raise_on_mismatch=True,
        )


async def test_delete_if_match_appends_a_tombstone_when_current(articles):
    deleted = await articles.delete_if_match(Article.id == 1, expected_version=3)

    assert (deleted.version, deleted.tombstone) == (4, True)


async def test_delete_if_match_refuses_a_stale_version(articles):
    assert await articles.delete_if_match(Article.id == 1, expected_version=1) is None


@pytest.mark.usefixtures("articles")
async def test_a_duplicate_version_collides_on_the_primary_key(raw):
    with pytest.raises(IntegrityError):
        await raw.insert([{"id": 1, "version": 3, "name": "racing"}]).execute()


# --- purging superseded versions ---


async def test_purge_keeps_the_newest_version_only(articles, raw):
    await articles.purge(id=1)

    assert [(row.version, row.name) for row in await raw.list()] == [(3, "final")]
    assert (await articles.get(id=1)).name == "final"


async def test_purge_leaves_other_entities_alone(articles, raw):
    await articles.create(id=2, name="other")
    await articles.update_one({"name": "other2"}, Article.id == 2)

    await articles.purge(id=1)

    assert sorted((row.id, row.version) for row in await raw.list()) == [
        (1, 3),
        (2, 1),
        (2, 2),
    ]


async def test_purge_keeps_a_tombstoned_head(articles, raw):
    await articles.remove(Article.id == 1)

    await articles.purge(id=1)

    assert [(row.version, row.tombstone) for row in await raw.list()] == [(4, True)]


async def test_purge_on_a_single_version_entity_is_a_no_op(repository, raw):
    await repository.create(id=1, name="only")

    await repository.purge(id=1)

    assert len(await raw.list()) == 1


# --- relationships ---


async def test_pinned_relationship_stays_on_its_version(articles):
    comments = CommentRepository()
    await comments.create(id=1, article_id=1, article_version=2, body="on the draft")

    await articles.update_one({"name": "even later"}, Article.id == 1)
    comment = await comments.load(Comment.article).one()

    assert (comment.article.version, comment.article.name) == (2, "revised")


async def test_latest_relationship_follows_the_entity_forward(articles):
    follows = FollowRepository()
    await follows.create(id=1, article_id=1)

    before = await follows.load(Follow.article).one()
    assert (before.article.version, before.article.name) == (3, "final")

    await articles.update_one({"name": "newest"}, Article.id == 1)
    after = await FollowRepository().load(Follow.article).one()

    assert (after.article.version, after.article.name) == (4, "newest")


@pytest.mark.usefixtures("articles")
async def test_latest_relationship_joins_without_eager_loading():
    follows = FollowRepository()
    await follows.create(id=1, article_id=1)

    rows = await follows.select(Article.name).join(Follow.article).all()

    assert rows == ["final"]


# --- scope escapes keep the routing preference ---


async def test_scope_escapes_retain_the_routing_preference(repository):
    other = Database(MEMORY_URL)
    try:
        bound = repository.using(db=other)

        assert bound.versions().db is other
        assert bound.at(sa.func.now()).db is other
        assert bound.with_deleted().db is other
    finally:
        await other.dispose()


# --- the UUIDv7 strategy behaves the same ---


async def test_uuid_strategy_appends_a_sortable_version():
    repository = UUIDArticleRepository()
    entity_id = uuid4()

    await repository.create(id=entity_id, name="draft")
    await repository.update_one({"name": "final"}, UUIDArticle.id == entity_id)

    history = await repository.history(id=entity_id)
    versions = [row.version for row in history]

    assert [row.name for row in history] == ["draft", "final"]
    assert all(isinstance(version, UUID) for version in versions)
    assert versions == sorted(versions)


async def test_uuid_strategy_reads_the_newest_version():
    repository = UUIDArticleRepository()
    entity_id = uuid4()

    await repository.create(id=entity_id, name="draft")
    await repository.update_one({"name": "final"}, UUIDArticle.id == entity_id)

    assert (await repository.get(id=entity_id)).name == "final"
    assert await repository.count() == 1
    assert await repository.versions().count() == 2


async def test_uuid_strategy_appends_client_side_in_bulk():
    """The bulk paths mint the successor in Python, not in SQL."""
    repository = UUIDArticleRepository()
    first, second = uuid4(), uuid4()
    await repository.create(id=first, name="a")
    await repository.create(id=second, name="b")

    await repository.bulk_update(
        [{"id": first, "name": "a2"}, {"id": second, "name": "b2"}]
    )

    assert (await repository.get(id=first)).name == "a2"
    assert (await repository.get(id=second)).name == "b2"
    versions = [row.version for row in await repository.history(id=first)]
    assert versions == sorted(versions)


async def test_uuid_strategy_bulk_create_or_update_appends_and_creates():
    repository = UUIDArticleRepository()
    known, fresh = uuid4(), uuid4()
    await repository.create(id=known, name="a")

    written = await repository.bulk_create_or_update(
        [{"id": known, "name": "a2"}, {"id": fresh, "name": "new"}],
        return_results=True,
    )

    assert len(written) == 2
    assert (await repository.get(id=known)).name == "a2"
    assert (await repository.get(id=fresh)).name == "new"
    assert await repository.versions().count() == 3


async def test_uuid_strategy_deletes_by_appending_a_tombstone():
    repository = UUIDArticleRepository()
    entity_id = uuid4()
    await repository.create(id=entity_id, name="draft")

    await repository.remove(UUIDArticle.id == entity_id)

    assert await repository.list() == []
    assert len(await repository.history(id=entity_id)) == 2


# --- a dialect without RETURNING has to find the appended rows again ---


class NoReturningQueryBuilder(SQLiteQueryBuilder):
    supported_options = Option.CONFLICTS


@pytest.fixture
def no_returning(db: Database):
    """Strip the RETURNING clause off the dialect, as MySQL has none."""
    builder = db.query_builder
    db.query_builder = NoReturningQueryBuilder()
    yield db
    db.query_builder = builder


@pytest.mark.usefixtures("no_returning")
async def test_the_fallback_tests_see_a_dialect_without_returning(repository):
    """Guards every test below: without this they exercise the native path."""
    assert not repository.qb.supports(Option.RETURNING)


@pytest.mark.usefixtures("no_returning")
async def test_update_one_refetches_the_appended_row(articles, raw):
    appended = await articles.update_one({"name": "refetched"}, Article.id == 1)

    assert (appended.version, appended.name) == (4, "refetched")
    assert len(await raw.list()) == 4


@pytest.mark.usefixtures("no_returning")
async def test_delete_one_refetches_the_tombstoned_row(articles):
    deleted = await articles.delete_one(Article.id == 1)

    assert (deleted.version, deleted.tombstone) == (4, True)
    assert await articles.get(id=1) is None


@pytest.mark.usefixtures("no_returning")
async def test_update_many_refetches_every_appended_row(repository):
    await repository.bulk_create([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    appended = await repository.update_many({"tag": "t"}, Article.id.in_([1, 2]))

    assert sorted((row.id, row.version, row.tag) for row in appended) == [
        (1, 2, "t"),
        (2, 2, "t"),
    ]


@pytest.mark.usefixtures("no_returning")
async def test_an_append_matching_nothing_returns_nothing(repository):
    assert await repository.update_one({"name": "x"}, Article.id == 404) is None


@pytest.mark.usefixtures("no_returning")
async def test_update_if_match_still_guards_without_returning(articles):
    assert (
        await articles.update_if_match(
            {"name": "stale"}, Article.id == 1, expected_version=1
        )
        is None
    )


# --- streaming ---


async def test_stream_yields_the_newest_versions(articles):
    rows = [row async for row in articles.select().stream()]

    assert [row[0].name for row in rows] == ["final"]


async def test_stream_runs_a_staged_append(articles, raw):
    staged = articles.update({"name": "streamed"}, return_results=True).filter(
        Article.id == 1
    )

    rows = [row async for row in staged.stream()]

    assert [row[0].name for row in rows] == ["streamed"]
    assert len(await raw.list()) == 4


async def test_bulk_update_of_nothing_is_a_no_op(repository, raw):
    await repository.bulk_update([])

    assert await raw.list() == []


# --- relationship helpers ---


@pytest.mark.parametrize("helper", [latest_relationship, version_foreign_key])
def test_relationship_helpers_check_the_column_count(helper):
    with pytest.raises(ValueError, match="identified by \\('id',\\)"):
        helper(Article, "article_id", "extra", "surplus")


def test_version_mapped_column_takes_the_type_of_the_parent():
    assert isinstance(Comment.__table__.c.article_version.type, sa.Integer)
    assert isinstance(UUIDArticle.__table__.c.version.type, GUID)


def test_the_marker_mixin_declares_no_version_strategy():
    with pytest.raises(NotImplementedError):
        AuditableMixin.next_version_expression()
