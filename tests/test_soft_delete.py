import pytest
import sqlalchemy as sa

from sqlargon import (
    Base,
    Database,
    DeletedRowExistsError,
    SoftDeleteBase,
    SoftDeleteRepository,
    SQLAlchemyRepository,
)
from sqlargon.mixins import SoftDeleteMixin
from tests import MEMORY_URL

# Models defined at module level to avoid re-registration with --count=3


class Article(SoftDeleteBase):
    __tablename__ = "test_soft_delete_article"
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Unicode(255), nullable=True)


class MixedIn(SoftDeleteMixin, Base):
    """The mixin combined with ``Base`` by hand, rather than SoftDeleteBase."""

    __tablename__ = "test_soft_delete_mixed_in"
    id = sa.Column(sa.Integer, primary_key=True)


class Plain(Base):
    __tablename__ = "test_soft_delete_plain"
    id = sa.Column(sa.Integer, primary_key=True)


class ArticleRepository(SoftDeleteRepository[Article]):
    default_order_by = Article.id


class RawArticleRepository(SQLAlchemyRepository[Article]):
    """Unscoped view of the same table, to observe what is physically stored."""

    default_order_by = Article.id


@pytest.fixture(autouse=True)
async def tables(db: Database):
    async with db.engine.begin() as conn:
        await conn.run_sync(Article.__table__.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        await conn.run_sync(Article.__table__.drop, checkfirst=True)


@pytest.fixture
def repository():
    return ArticleRepository()


@pytest.fixture
def raw():
    return RawArticleRepository()


@pytest.fixture
async def articles(repository):
    await repository.bulk_create([{"id": 1, "name": "kept"}, {"id": 2, "name": "gone"}])
    await repository.remove(Article.id == 2)
    return repository


# --- model validation ---


def test_model_without_soft_delete_mixin_is_rejected():
    with pytest.raises(TypeError, match="Plain must inherit from SoftDeleteMixin"):

        class BadRepository(SoftDeleteRepository[Plain]):  # type: ignore[type-var]
            pass


def test_model_declaring_the_mixin_by_hand_is_accepted():
    # the static bound asks for SoftDeleteBase, but the tombstone column and
    # its hybrids are all the repository actually needs
    class MixedInRepository(SoftDeleteRepository[MixedIn]):  # type: ignore[type-var]
        pass

    assert MixedInRepository.model is MixedIn


def test_abstract_subclass_needs_no_model():
    class Shared(SoftDeleteRepository[Article], abstract=True):
        pass

    class Concrete(Shared):
        pass

    assert Concrete.model is Article


def test_model_can_be_passed_explicitly():
    class Explicit(SoftDeleteRepository, model=Article):
        pass

    assert Explicit.model is Article


# --- delete raises the tombstone ---


async def test_remove_tombstones_instead_of_deleting(repository, raw):
    await repository.create(id=1, name="john")

    await repository.remove(Article.id == 1)

    assert await repository.count() == 0
    stored = await raw.select().one()
    assert stored.tombstone is True


async def test_delete_one_returns_the_tombstoned_row(repository, raw):
    await repository.create(id=1, name="john")

    article = await repository.delete_one(Article.id == 1)

    assert article is not None
    assert article.tombstone is True
    assert await raw.count() == 1


async def test_delete_many_tombstones_every_match(repository, raw):
    await repository.bulk_create([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    await repository.delete_many(Article.name == "a")

    assert [row.id for row in await repository.all()] == [2]
    assert await raw.count() == 2


async def test_deleting_a_deleted_row_is_a_no_op(articles):
    assert await articles.delete_one(Article.id == 2) is None


async def test_hard_delete_removes_the_row(repository, raw):
    await repository.create(id=1, name="john")

    await repository.hard_delete(Article.id == 1)

    assert await raw.count() == 0


async def test_hard_delete_reaches_tombstoned_rows(articles, raw):
    await articles.hard_delete(Article.is_deleted)

    assert [row.id for row in await raw.all()] == [1]


# --- reads are scoped to live rows ---


@pytest.mark.parametrize("name", ["all", "list"])
async def test_reads_skip_tombstoned_rows(articles, name):
    rows = await getattr(articles, name)()

    assert [row.id for row in rows] == [1]


async def test_get_skips_tombstoned_rows(articles):
    assert await articles.get(id=2) is None
    assert await articles.get(id=1) is not None


async def test_count_skips_tombstoned_rows(articles):
    assert await articles.count() == 1
    assert await articles.count(name="gone") == 0


async def test_select_on_columns_is_scoped(articles):
    rows = (await articles.select(Article.name).mappings()).all()

    assert [row["name"] for row in rows] == ["kept"]


async def test_filter_on_the_default_query_is_scoped(articles):
    assert await articles.filter(name="gone").all() == []


async def test_default_order_by_survives_the_scope(articles, repository):
    await repository.create(id=3, name="third")

    assert [row.id for row in await articles.all()] == [1, 3]


# --- updates are scoped to live rows ---


async def test_update_does_not_touch_tombstoned_rows(articles, raw):
    await articles.update_many({"name": "renamed"})

    stored = {row.id: row.name for row in await raw.all()}
    assert stored == {1: "renamed", 2: "gone"}


async def test_update_one_on_a_tombstoned_row_returns_none(articles):
    assert await articles.update_one({"name": "renamed"}, Article.id == 2) is None


async def test_bulk_update_does_not_touch_tombstoned_rows(articles, raw):
    await articles.bulk_update(
        [{"id": 1, "name": "renamed"}, {"id": 2, "name": "renamed"}]
    )

    stored = {row.id: row.name for row in await raw.all()}
    assert stored == {1: "renamed", 2: "gone"}


async def test_upsert_does_not_resurrect_a_tombstoned_row(articles, raw):
    await articles.create_or_update(id=2, name="back?")

    stored = await raw.get(id=2)
    assert stored.name == "back?"
    assert stored.tombstone is True


def test_tombstone_is_excluded_from_the_default_conflict_set():
    assert "tombstone" not in ArticleRepository._get_default_set()
    assert "tombstone" in RawArticleRepository._get_default_set()


async def test_get_chunk_for_update_tombstones_the_chunk(articles, raw):
    async with articles.get_chunk_for_update(None) as chunk:
        assert [row.id for row in chunk] == [1]

    assert await articles.count() == 0
    assert await raw.count() == 2


# --- reaching past the scope ---


async def test_with_deleted_covers_every_row(articles):
    rows = await articles.with_deleted().all()

    assert [row.id for row in rows] == [1, 2]
    assert await articles.with_deleted().count() == 2


async def test_with_deleted_survives_chaining(articles):
    rows = await articles.with_deleted().filter(name="gone").all()

    assert [row.id for row in rows] == [2]


async def test_with_deleted_leaves_the_original_scoped(articles):
    assert await articles.with_deleted().count() == 2
    assert await articles.count() == 1


async def test_with_deleted_keeps_the_routing_preference(articles):
    other_db = Database(MEMORY_URL)

    assert articles.using(db=other_db).with_deleted().db is other_db


async def test_only_deleted_returns_the_tombstoned_rows(articles):
    rows = await articles.only_deleted().all()

    assert [row.id for row in rows] == [2]


async def test_only_deleted_counts_the_tombstoned_rows(articles):
    assert await articles.only_deleted().count() == 1
    assert await articles.only_deleted().count(name="kept") == 0


async def test_only_deleted_updates_the_tombstoned_rows(articles, raw):
    updated = await articles.only_deleted().update_many({"name": "renamed"})

    assert [row.id for row in updated] == [2]
    stored = {row.id: row.name for row in await raw.all()}
    assert stored == {1: "kept", 2: "renamed"}


async def test_only_deleted_bulk_updates_the_tombstoned_rows(articles, raw):
    await articles.only_deleted().bulk_update(
        [{"id": 1, "name": "renamed"}, {"id": 2, "name": "renamed"}]
    )

    stored = {row.id: row.name for row in await raw.all()}
    assert stored == {1: "kept", 2: "renamed"}


async def test_only_deleted_hard_delete_empties_the_trash(articles, raw):
    await articles.only_deleted().hard_delete()

    assert [row.id for row in await raw.all()] == [1]


async def test_only_deleted_leaves_the_original_scoped(articles):
    assert await articles.only_deleted().count() == 1
    assert await articles.count() == 1
    assert [row.id for row in await articles.all()] == [1]


async def test_with_deleted_widens_an_only_deleted_scope(articles):
    assert await articles.only_deleted().with_deleted().count() == 2


async def test_restore_clears_the_tombstone(articles):
    restored = await articles.restore(Article.id == 2)

    assert [row.id for row in restored] == [2]
    assert [row.id for row in await articles.all()] == [1, 2]


async def test_restore_only_returns_rows_that_were_deleted(articles):
    restored = await articles.restore(Article.id == 1)

    assert list(restored) == []


async def test_restore_accepts_keyword_filters(articles):
    restored = await articles.restore(name="gone")

    assert [row.id for row in restored] == [2]


# --- get_or_create ---


async def test_get_or_create_creates_a_missing_row(repository):
    article = await repository.get_or_create({"name": "new"}, id=1)

    assert article.name == "new"
    assert await repository.count() == 1


async def test_get_or_create_returns_the_live_row(articles):
    article = await articles.get_or_create(id=1)

    assert article.name == "kept"


async def test_get_or_create_refuses_to_resurrect_a_tombstoned_row(articles):
    with pytest.raises(DeletedRowExistsError, match="A deleted Article row"):
        await articles.get_or_create(id=2)
