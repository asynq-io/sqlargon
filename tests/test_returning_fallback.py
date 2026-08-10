"""Repository methods on a dialect that has no RETURNING clause.

SQLite stands in for one: its query builder is swapped for a copy claiming
no :attr:`~sqlargon.query_builder.Option.RETURNING`, so every method that
hands models back has to re-read the rows it wrote.
"""

from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Mapped, mapped_column

from sqlargon import (
    Base,
    Database,
    DeletedRowExistsError,
    SoftDeleteBase,
    SoftDeleteRepository,
    SQLAlchemyRepository,
)
from sqlargon.dialects.sqlite import SQLiteQueryBuilder
from sqlargon.query_builder import Option, UnsupportedOption
from sqlargon.types import GUID, GenerateUUID
from sqlargon.typing import OnConflictOptions


class NoReturningQueryBuilder(SQLiteQueryBuilder):
    supported_options = Option.CONFLICTS


# Declared at module level: pytest-repeat re-runs every test, and a model
# declared inside a test would clash with itself on the second run.
class Fruit(Base):
    __tablename__ = "fallback_fruit"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(sa.Unicode(64), unique=True)
    score: Mapped[int] = mapped_column(sa.Integer, default=0)


class ServerKeyed(Base):
    """A model whose key only the server can fill in."""

    __tablename__ = "fallback_server_keyed"

    id: Mapped[UUID] = mapped_column(
        GUID(), primary_key=True, server_default=GenerateUUID()
    )
    name: Mapped[str] = mapped_column(sa.Unicode(64))


class Crate(Base):
    """A model named by a composite key rather than a single column."""

    __tablename__ = "fallback_crate"

    region: Mapped[str] = mapped_column(sa.Unicode(16), primary_key=True)
    slot: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    label: Mapped[str] = mapped_column(sa.Unicode(64))


class SoftFruit(SoftDeleteBase):
    __tablename__ = "fallback_soft_fruit"

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(sa.Unicode(64), unique=True)


TABLES = [
    Base.metadata.tables[model.__tablename__]
    for model in (Fruit, ServerKeyed, Crate, SoftFruit)
]


class FruitRepository(SQLAlchemyRepository[Fruit]):
    pass


class FruitByNameRepository(FruitRepository):
    @property
    def on_conflict(self) -> OnConflictOptions:
        return {"index_elements": {"name"}, "set_": {"score"}}


class ServerKeyedRepository(SQLAlchemyRepository[ServerKeyed]):
    pass


class CrateRepository(SQLAlchemyRepository[Crate]):
    pass


class SoftFruitRepository(SoftDeleteRepository[SoftFruit]):
    pass


class SoftFruitByNameRepository(SoftFruitRepository):
    @property
    def on_conflict(self) -> OnConflictOptions:
        return {"index_elements": {"name"}, "set_": {"name"}}


@pytest.fixture(autouse=True)
async def no_returning(db: Database):
    """Strip the RETURNING clause off the dialect for the whole module."""
    builder = db.query_builder
    db.query_builder = NoReturningQueryBuilder()
    async with db.engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=TABLES)
    yield db
    db.query_builder = builder
    async with db.engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all, tables=TABLES)


@pytest.fixture
def fruits() -> FruitRepository:
    return FruitRepository()


@pytest.fixture
def soft_fruits() -> SoftFruitRepository:
    return SoftFruitRepository()


async def test_the_repository_sees_a_dialect_without_returning(
    fruits: FruitRepository,
):
    """Guards every test below: without this they exercise the native path."""
    assert not fruits.qb.supports(Option.RETURNING)
    assert fruits.qb.supports(Option.CONFLICTS)


async def test_create_returns_the_inserted_row(fruits: FruitRepository):
    fruit = await fruits.create(name="apple")
    assert fruit is not None
    assert fruit.name == "apple"
    assert fruit.score == 0
    assert await fruits.get(id=fruit.id) is not None


async def test_create_returns_none_on_conflict(fruits: FruitRepository):
    fruit_id = uuid4()
    assert await fruits.create(id=fruit_id, name="apple") is not None
    assert await fruits.create(id=fruit_id, name="pear") is None
    assert await fruits.count() == 1


async def test_create_or_update_is_idempotent(fruits: FruitRepository):
    fruit_id = uuid4()
    first = await fruits.create_or_update(id=fruit_id, name="apple")
    second = await fruits.create_or_update(id=fruit_id, name="pear")
    assert first.id == second.id == fruit_id
    assert second.name == "pear"
    assert await fruits.count() == 1


async def test_get_or_create_returns_the_stored_row():
    repository = FruitByNameRepository()
    created = await repository.get_or_create({"score": 5}, name="apple")
    existing = await repository.get_or_create({"score": 9}, name="apple")
    assert created.id == existing.id
    assert existing.score == 5


async def test_create_many_returns_every_row(fruits: FruitRepository):
    names = ("apple", "pear", "plum")
    created = await fruits.create_many([{"name": name} for name in names])
    assert sorted(fruit.name for fruit in created) == sorted(names)


async def test_bulk_create_returns_only_the_inserted_rows(fruits: FruitRepository):
    stored = await fruits.create(name="apple")
    assert stored is not None
    created = await fruits.bulk_create(
        [{"id": stored.id, "name": "apple"}, {"name": "pear"}], return_results=True
    )
    assert [fruit.name for fruit in created] == ["pear"]


async def test_bulk_create_or_update_returns_every_row(fruits: FruitRepository):
    await fruits.create_many([{"name": "apple"}, {"name": "pear"}])
    rows = await fruits.list()
    updated = await fruits.bulk_create_or_update(
        [{"id": row.id, "name": row.name, "score": 8} for row in rows],
        return_results=True,
    )
    assert {row.score for row in updated} == {8}


async def test_update_one_returns_the_updated_row(fruits: FruitRepository):
    await fruits.create(name="apple")
    updated = await fruits.update_one({"score": 4}, Fruit.name == "apple")
    assert updated is not None
    assert updated.score == 4


async def test_update_one_rewriting_the_filtered_column(fruits: FruitRepository):
    """The rows are named before the update, which may move them out of it."""
    await fruits.create(name="apple")
    updated = await fruits.update_one({"name": "pear"}, Fruit.name == "apple")
    assert updated is not None
    assert updated.name == "pear"


async def test_update_one_without_a_match(fruits: FruitRepository):
    assert await fruits.update_one({"score": 4}, Fruit.name == "apple") is None


async def test_update_one_refuses_more_than_one_row(fruits: FruitRepository):
    await fruits.create_many([{"name": "apple"}, {"name": "pear"}])
    with pytest.raises(MultipleResultsFound):
        await fruits.update_one({"score": 4})


async def test_update_many_returns_every_updated_row(fruits: FruitRepository):
    await fruits.create_many([{"name": "apple"}, {"name": "pear"}])
    updated = await fruits.update_many({"score": 2})
    assert len(updated) == 2
    assert {fruit.score for fruit in updated} == {2}


async def test_delete_one_returns_the_deleted_row(fruits: FruitRepository):
    await fruits.create(name="apple")
    deleted = await fruits.delete_one(Fruit.name == "apple")
    assert deleted is not None
    assert deleted.name == "apple"
    assert await fruits.count() == 0


async def test_delete_one_without_a_match(fruits: FruitRepository):
    assert await fruits.delete_one(Fruit.name == "apple") is None


async def test_a_composite_key_names_the_written_rows():
    crates = CrateRepository()
    created = await crates.create_many(
        [
            {"region": "eu", "slot": 1, "label": "apples"},
            {"region": "us", "slot": 1, "label": "pears"},
        ]
    )
    assert sorted(crate.region for crate in created) == ["eu", "us"]
    updated = await crates.update_one({"label": "plums"}, Crate.region == "eu")
    assert updated is not None
    assert (updated.slot, updated.label) == (1, "plums")
    deleted = await crates.delete_one(Crate.region == "us")
    assert deleted is not None
    assert deleted.label == "pears"
    assert await crates.count() == 1


async def test_a_server_generated_key_cannot_be_read_back():
    repository = ServerKeyedRepository()
    with pytest.raises(UnsupportedOption, match=r"ServerKeyed\.id"):
        await repository.create(name="apple")


async def test_delete_one_returns_the_tombstoned_row(soft_fruits: SoftFruitRepository):
    created = await soft_fruits.create(name="apple")
    assert created is not None
    deleted = await soft_fruits.delete_one(SoftFruit.name == "apple")
    assert deleted is not None
    assert deleted.tombstone is True
    assert await soft_fruits.count() == 0


async def test_restore_returns_the_restored_rows(soft_fruits: SoftFruitRepository):
    created = await soft_fruits.create(name="apple")
    assert created is not None
    await soft_fruits.remove(SoftFruit.id == created.id)
    restored = await soft_fruits.restore(SoftFruit.id == created.id)
    assert [fruit.name for fruit in restored] == ["apple"]
    assert await soft_fruits.count() == 1


async def test_get_or_create_refuses_to_reuse_a_deleted_row():
    repository = SoftFruitByNameRepository()
    created = await repository.get_or_create(name="apple")
    await repository.remove(SoftFruit.id == created.id)
    with pytest.raises(DeletedRowExistsError):
        await repository.get_or_create(name="apple")
