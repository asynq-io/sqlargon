"""Unit tests for VersionedRepository and the versioning mixins.

Mirrors ``test_soft_delete.py``: in-memory SQLite via the shared ``db``
fixture, module-level models to survive ``--count=3`` re-registration.
"""

from typing import Any
from uuid import UUID

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import declared_attr

from sqlargon import (
    Base,
    ConcurrentModificationError,
    Database,
    SQLAlchemyRepository,
    UUIDVersionedMixin,
    VersionedBase,
    VersionedMixin,
    VersionedRepository,
    XminVersionedBase,
)
from sqlargon.mixins import UUIDModelMixin
from sqlargon.types import GUID, GenerateUUID

# Models defined at module level to avoid re-registration with --count=3


class VersionedArticle(UUIDModelMixin, VersionedBase):
    __tablename__ = "test_versioned_article"
    name = sa.Column(sa.Unicode(255), nullable=True)


class HandVersioned(UUIDVersionedMixin, Base):
    """The mixin combined with ``Base`` by hand, rather than VersionedBase."""

    __tablename__ = "test_versioned_hand"
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Unicode(255), nullable=True)


class PlainModel(Base):
    __tablename__ = "test_versioned_plain"
    id = sa.Column(sa.Integer, primary_key=True)


class MarkerOnly(VersionedMixin, Base):
    """The abstract marker, with none of the columns its subclasses map."""

    __tablename__ = "test_versioned_marker"
    id = sa.Column(sa.Integer, primary_key=True)


class CounterVersioned(VersionedMixin, Base):
    """An integer version column, the counter SQLAlchemy versions by default."""

    __tablename__ = "test_versioned_counter"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.Unicode(255), nullable=True)
    version_id = sa.Column(sa.Integer, nullable=False, default=1)

    @declared_attr.directive
    def __mapper_args__(cls) -> dict[str, Any]:
        return {"eager_defaults": True, "version_id_col": cls.version_id}


class ServerVersioned(XminVersionedBase):
    """A server managed version, which the repository must never set itself."""

    __tablename__ = "test_versioned_server"

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.Unicode(255), nullable=True)


class VersionedArticleRepository(VersionedRepository[VersionedArticle]):
    default_order_by = VersionedArticle.id


class RawArticleRepository(SQLAlchemyRepository[VersionedArticle]):
    """Unscoped view of the same table, to observe what is physically stored."""

    default_order_by = VersionedArticle.id


class HandVersionedRepository(VersionedRepository[HandVersioned]):  # type: ignore[type-var]
    pass


class CounterRepository(VersionedRepository[CounterVersioned]):  # type: ignore[type-var]
    default_order_by = CounterVersioned.id


class ServerVersionedRepository(VersionedRepository[ServerVersioned]):
    pass


# --- fixtures ---


@pytest.fixture(autouse=True)
async def tables(db: Database):
    created = (VersionedArticle.__table__, CounterVersioned.__table__)
    async with db.engine.begin() as conn:
        for table in created:
            await conn.run_sync(table.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        for table in reversed(created):
            await conn.run_sync(table.drop, checkfirst=True)


@pytest.fixture
def counters():
    return CounterRepository()


@pytest.fixture
def repository():
    return VersionedArticleRepository()


@pytest.fixture
def raw():
    return RawArticleRepository()


@pytest.fixture
async def article(repository):
    return await repository.create(name="john")


# --- model validation ---


def test_model_without_versioned_mixin_is_rejected():
    with pytest.raises(TypeError, match="PlainModel must inherit from VersionedMixin"):

        class BadRepository(VersionedRepository[PlainModel]):  # type: ignore[type-var]
            pass


def test_model_without_version_column_is_rejected():
    # the marker alone maps nothing to compare, so every guard would match
    # zero rows and report a concurrent modification that never happened
    with pytest.raises(TypeError, match="MarkerOnly maps no version column"):

        class BadRepository(VersionedRepository[MarkerOnly]):  # type: ignore[type-var]
            pass


def test_model_declaring_the_mixin_by_hand_is_accepted():
    # the static bound asks for VersionedBase, but the version column and
    # its mapper args are all the repository actually needs
    assert HandVersionedRepository.model is HandVersioned


def test_abstract_subclass_needs_no_model():
    class Shared(VersionedRepository[VersionedArticle], abstract=True):
        pass

    class Concrete(Shared):
        pass

    assert Concrete.model is VersionedArticle


def test_model_can_be_passed_explicitly():
    class Explicit(VersionedRepository, model=VersionedArticle):
        pass

    assert Explicit.model is VersionedArticle


# --- column & mapper ---


def test_uuid_version_column():
    column = VersionedArticle.__table__.c.version_id
    assert isinstance(column.type, GUID)
    assert not column.nullable
    assert column.default.is_callable
    assert isinstance(column.server_default.arg, GenerateUUID)


def test_mapper_version_id_col():
    assert (
        VersionedArticle.__mapper__.version_id_col
        is VersionedArticle.__table__.c.version_id
    )


def test_mapper_version_id_generator():
    generator = VersionedArticle.__mapper__.version_id_generator
    assert callable(generator)
    # generator ignores the incoming version and returns a fresh UUID
    assert isinstance(generator(None), UUID)


def test_version_excluded_from_default_conflict_set():
    assert "version_id" not in VersionedArticleRepository._get_default_set()
    assert "version_id" in RawArticleRepository._get_default_set()


# --- auto-increment on update ---


@pytest.mark.usefixtures("tables")
async def test_create_sets_initial_version(repository):
    obj = await repository.create(name="john")

    assert isinstance(obj.version_id, UUID)


@pytest.mark.usefixtures("tables")
async def test_update_one_increments_version(repository, article):
    updated = await repository.update_one(
        {"name": "jane"}, VersionedArticle.id == article.id
    )

    assert updated is not None
    assert updated.version_id != article.version_id


@pytest.mark.usefixtures("tables")
async def test_update_many_increments_version(repository, raw):
    await repository.bulk_create([{"name": "a"}, {"name": "b"}], return_results=False)
    rows = await raw.all()
    original = {row.id: row.version_id for row in rows}

    await repository.update_many(
        {"name": "renamed"}, VersionedArticle.name.in_(["a", "b"])
    )

    updated = {row.id: row.version_id for row in await raw.all()}
    assert all(updated[rid] != original[rid] for rid in original)


@pytest.mark.usefixtures("tables")
async def test_bulk_update_increments_version(repository, raw):
    await repository.bulk_create([{"name": "a"}, {"name": "b"}], return_results=False)
    rows = await raw.all()
    original = {row.id: row.version_id for row in rows}

    await repository.bulk_update([{"id": row.id, "name": "renamed"} for row in rows])

    updated = {row.id: row.version_id for row in await raw.all()}
    assert all(updated[rid] != original[rid] for rid in original)


# --- update_if_match ---


@pytest.mark.usefixtures("tables")
async def test_update_if_match_returns_row_on_matching_version(repository, article):
    updated = await repository.update_if_match(
        {"name": "jane"},
        VersionedArticle.id == article.id,
        expected_version=article.version_id,
    )

    assert updated is not None
    assert updated.name == "jane"
    assert updated.version_id != article.version_id


@pytest.mark.usefixtures("tables")
async def test_update_if_match_returns_none_on_mismatched_version(repository, article):
    from uuid_utils.compat import uuid4 as _uuid4  # noqa: PLC0415

    wrong = _uuid4()
    updated = await repository.update_if_match(
        {"name": "jane"},
        VersionedArticle.id == article.id,
        expected_version=wrong,
    )

    assert updated is None


@pytest.mark.usefixtures("tables")
async def test_update_if_match_raises_on_mismatch_when_configured(repository, article):
    from uuid_utils.compat import uuid4 as _uuid4  # noqa: PLC0415

    wrong = _uuid4()
    with pytest.raises(ConcurrentModificationError, match="was modified or deleted"):
        await repository.update_if_match(
            {"name": "jane"},
            VersionedArticle.id == article.id,
            expected_version=wrong,
            raise_on_mismatch=True,
        )


@pytest.mark.usefixtures("tables")
async def test_update_if_match_accepts_kwargs(repository, article):
    updated = await repository.update_if_match(
        {"name": "jane"},
        expected_version=article.version_id,
        id=article.id,
    )

    assert updated is not None
    assert updated.name == "jane"


@pytest.mark.usefixtures("tables")
async def test_update_if_match_increments_version(repository, article):
    updated = await repository.update_if_match(
        {"name": "jane"},
        VersionedArticle.id == article.id,
        expected_version=article.version_id,
    )

    assert updated is not None
    assert updated.version_id != article.version_id


# --- delete_if_match ---


@pytest.mark.usefixtures("tables")
async def test_delete_if_match_returns_row_on_matching_version(repository, article):
    deleted = await repository.delete_if_match(
        VersionedArticle.id == article.id,
        expected_version=article.version_id,
    )

    assert deleted is not None
    assert deleted.name == "john"
    assert await repository.count() == 0


@pytest.mark.usefixtures("tables")
async def test_delete_if_match_returns_none_on_mismatched_version(repository, article):
    from uuid_utils.compat import uuid4 as _uuid4  # noqa: PLC0415

    wrong = _uuid4()
    deleted = await repository.delete_if_match(
        VersionedArticle.id == article.id,
        expected_version=wrong,
    )

    assert deleted is None
    assert await repository.count() == 1


@pytest.mark.usefixtures("tables")
async def test_delete_if_match_raises_on_mismatch_when_configured(repository, article):
    from uuid_utils.compat import uuid4 as _uuid4  # noqa: PLC0415

    wrong = _uuid4()
    with pytest.raises(ConcurrentModificationError, match="was modified or deleted"):
        await repository.delete_if_match(
            VersionedArticle.id == article.id,
            expected_version=wrong,
            raise_on_mismatch=True,
        )


@pytest.mark.usefixtures("tables")
async def test_delete_if_match_accepts_kwargs(repository, article):
    deleted = await repository.delete_if_match(
        expected_version=article.version_id,
        id=article.id,
    )

    assert deleted is not None
    assert deleted.name == "john"


# --- upsert / conflict ---


@pytest.mark.usefixtures("tables")
async def test_upsert_does_not_clobber_version(repository, raw, article):
    original = article.version_id

    await repository.create_or_update(id=article.id, name="back?")

    stored = await raw.get(id=article.id)
    assert stored is not None
    assert stored.name == "back?"
    assert stored.version_id == original


@pytest.mark.usefixtures("tables")
async def test_create_or_update_sets_version_on_insert(repository):
    obj = await repository.create_or_update(name="fresh")

    assert obj is not None
    assert isinstance(obj.version_id, UUID)


# --- regular methods with manual filters ---


@pytest.mark.usefixtures("tables")
async def test_update_one_with_manual_version_filter_returns_none_on_mismatch(
    repository, article
):
    from uuid_utils.compat import uuid4 as _uuid4  # noqa: PLC0415

    wrong = _uuid4()
    updated = await repository.update_one(
        {"name": "jane"},
        VersionedArticle.id == article.id,
        VersionedArticle.version_id == wrong,
    )

    assert updated is None


@pytest.mark.usefixtures("tables")
async def test_update_one_with_manual_version_filter_works_on_match(
    repository, article
):
    updated = await repository.update_one(
        {"name": "jane"},
        VersionedArticle.id == article.id,
        VersionedArticle.version_id == article.version_id,
    )

    assert updated is not None
    assert updated.name == "jane"


# --- integer counter versions ---


def test_counter_version_is_recognised():
    assert CounterRepository._is_counter()
    assert not VersionedArticleRepository._is_counter()


def test_counter_update_bumps_with_a_sql_expression(counters):
    """A counter has no fresh value to bind: it is bumped relative to the row."""
    query = counters.update({"name": "jane"}).query

    assert "version_id + " in str(query.compile(compile_kwargs={"literal_binds": True}))


async def test_counter_version_starts_at_one(counters):
    row = await counters.create(name="john")

    assert row.version_id == 1


async def test_counter_version_increments_on_every_update(counters):
    row = await counters.create(name="john")

    for expected in (2, 3, 4):
        row = await counters.update_one({"name": "jane"}, CounterVersioned.id == row.id)
        assert row.version_id == expected


async def test_counter_update_if_match_rejects_a_stale_version(counters):
    row = await counters.create(name="john")
    await counters.update_one({"name": "jane"}, CounterVersioned.id == row.id)

    stale = await counters.update_if_match(
        {"name": "joan"}, CounterVersioned.id == row.id, expected_version=1
    )

    assert stale is None


async def test_counter_update_if_match_accepts_the_current_version(counters):
    row = await counters.create(name="john")

    updated = await counters.update_if_match(
        {"name": "jane"},
        CounterVersioned.id == row.id,
        expected_version=row.version_id,
    )

    assert updated is not None
    assert updated.version_id == 2


async def test_counter_bulk_update_leaves_the_version_alone(counters):
    """An executemany binds one set of parameters per row, which a SQL
    expression -- the same for every row -- is not.
    """
    first = await counters.create(name="a")
    second = await counters.create(name="b")

    await counters.bulk_update(
        [{"id": first.id, "name": "a2"}, {"id": second.id, "name": "b2"}]
    )

    rows = await counters.select().order_by(CounterVersioned.id).all()
    assert [row.name for row in rows] == ["a2", "b2"]
    assert [row.version_id for row in rows] == [1, 1]


# --- server managed versions ---


def test_server_managed_version_is_never_set():
    query = ServerVersionedRepository().update({"name": "jane"}).query

    assert "xmin" not in str(query.compile(compile_kwargs={"literal_binds": True}))


def test_server_managed_version_is_recognised():
    assert ServerVersionedRepository._is_server_versioned()
    assert not VersionedArticleRepository._is_server_versioned()


# --- multi row updates ---


def test_update_of_multiple_rows_versions_each_of_them(repository):
    versioned = repository._with_version_increment(
        [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    )

    versions = [row["version_id"] for row in versioned]
    assert all(isinstance(version, UUID) for version in versions)
    assert versions[0] != versions[1]


def test_counter_update_of_multiple_rows_leaves_the_version_alone(counters):
    values = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    assert counters._with_version_increment(values) == values


def test_server_managed_guard_compares_the_column_as_text():
    """``xid = varchar`` is not an operator PostgreSQL has."""
    guard = ServerVersionedRepository()._version_filter(999)

    sql = str(guard.compile(compile_kwargs={"literal_binds": True}))
    assert sql == "CAST(test_versioned_server.xmin AS TEXT) = '999'"
