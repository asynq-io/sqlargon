from datetime import datetime, timezone
from uuid import UUID

import pytest
import sqlalchemy as sa

from sqlargon import Database, SQLAlchemyRepository
from sqlargon.mixins import (
    CreatedUpdatedMixin,
    SoftDeleteMixin,
    UUIDModelMixin,
    UUIDV7ModelMixin,
)
from sqlargon.orm import Base as _Base
from sqlargon.types import GUID, GenerateUUID, GenerateUUIDV7, Timestamp, now

# Models defined at module level to avoid re-registration with --count=3

OLD = datetime(2020, 1, 1, tzinfo=timezone.utc)
NEWER = datetime(2021, 1, 1, tzinfo=timezone.utc)


class _MixinModel(UUIDModelMixin, CreatedUpdatedMixin, SoftDeleteMixin, _Base):
    __tablename__ = "test_mixins_model"
    name = sa.Column(sa.Unicode(255), nullable=True)


class _UUIDV7Model(UUIDV7ModelMixin, _Base):
    __tablename__ = "test_mixins_uuidv7"
    name = sa.Column(sa.Unicode(255), nullable=True)


class _MixinRepository(SQLAlchemyRepository[_MixinModel]):
    pass


@pytest.fixture
async def tables(db: Database):
    async with db.engine.begin() as conn:
        await conn.run_sync(_MixinModel.__table__.create, checkfirst=True)
        await conn.run_sync(_UUIDV7Model.__table__.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        await conn.run_sync(_MixinModel.__table__.drop, checkfirst=True)
        await conn.run_sync(_UUIDV7Model.__table__.drop, checkfirst=True)


@pytest.fixture
def repository():
    return _MixinRepository()


# --- column declarations ---


@pytest.mark.parametrize(
    ("model", "version", "server_default_type"),
    [
        (_MixinModel, 4, GenerateUUID),
        (_UUIDV7Model, 7, GenerateUUIDV7),
    ],
)
def test_uuid_mixin_id_column(model, version, server_default_type):
    column = model.__table__.c.id
    assert isinstance(column.type, GUID)
    assert column.primary_key
    assert not column.nullable
    assert column.default.is_callable
    assert column.default.arg(None).version == version
    assert isinstance(column.server_default.arg, server_default_type)


@pytest.mark.parametrize("column_name", ["created_at", "updated_at"])
def test_created_updated_columns_use_server_defaults(column_name):
    column = _MixinModel.__table__.c[column_name]
    assert isinstance(column.type, Timestamp)
    assert not column.nullable
    # timestamps are filled by the database, not client side
    assert column.default is None
    assert isinstance(column.server_default.arg, now)


def test_updated_at_column_has_onupdate():
    column = _MixinModel.__table__.c.updated_at
    assert isinstance(column.onupdate.arg, now)
    assert isinstance(column.server_onupdate.arg, now)


def test_created_at_column_has_no_onupdate():
    assert _MixinModel.__table__.c.created_at.onupdate is None


def test_tombstone_column():
    column = _MixinModel.__table__.c.tombstone
    assert isinstance(column.type, sa.Boolean)
    assert not column.nullable
    assert column.default.arg is False
    assert str(column.server_default.arg) == "false"


# --- inserts ---


@pytest.mark.usefixtures("tables")
async def test_uuid_mixin_generates_uuid4_on_insert(repository):
    obj = await repository.create(name="john")
    assert isinstance(obj.id, UUID)
    assert obj.id.version == 4


@pytest.mark.usefixtures("tables")
async def test_uuidv7_mixin_generates_uuid7_on_insert(db: Database):
    async with db.session() as session:
        obj = _UUIDV7Model(name="john")
        session.add(obj)
        await session.flush()

    assert isinstance(obj.id, UUID)
    assert obj.id.version == 7


@pytest.mark.usefixtures("tables")
async def test_created_updated_filled_by_server_default(repository):
    obj = await repository.create(name="john")

    assert obj.created_at.tzinfo == timezone.utc
    assert obj.updated_at.tzinfo == timezone.utc
    # a single evaluation of now() per statement fills both columns
    assert obj.created_at == obj.updated_at
    assert obj.is_new is True


@pytest.mark.usefixtures("tables")
async def test_bulk_insert_rows_are_new(repository):
    rows = await repository.bulk_create(
        [{"name": "john"}, {"name": "jane"}], return_results=True
    )

    assert len(rows) == 2
    assert all(row.created_at == row.updated_at for row in rows)
    assert all(row.is_new for row in rows)


@pytest.mark.usefixtures("tables")
async def test_server_defaults_round_trip(db: Database):
    async with db.session() as session:
        await session.execute(
            sa.text("INSERT INTO test_mixins_model (name) VALUES (:name)"),
            {"name": "john"},
        )

    async with db.session() as session:
        obj = (await session.execute(sa.select(_MixinModel))).scalars().one()

    assert isinstance(obj.id, UUID)
    assert obj.created_at == obj.updated_at
    assert obj.created_at.tzinfo == timezone.utc
    assert obj.tombstone is False
    assert obj.is_new is True


# --- is_new ---


@pytest.mark.usefixtures("tables")
async def test_is_new_instance_level(repository):
    new = await repository.create(name="new", created_at=OLD, updated_at=OLD)
    touched = await repository.create(name="touched", created_at=OLD, updated_at=NEWER)

    assert new.is_new is True
    assert touched.is_new is False


@pytest.mark.usefixtures("tables")
async def test_is_new_false_after_update(repository):
    obj = await repository.create(name="john", created_at=OLD, updated_at=OLD)
    assert obj.is_new is True

    updated = await repository.update_one({"name": "jane"}, _MixinModel.id == obj.id)

    assert updated.created_at == OLD
    assert updated.updated_at > updated.created_at
    assert updated.is_new is False


def test_is_new_sql_expression():
    assert (
        str(_MixinModel.is_new.expression)
        == "test_mixins_model.created_at = test_mixins_model.updated_at"
    )


@pytest.mark.usefixtures("tables")
async def test_is_new_as_sql_filter(repository):
    await repository.create(name="new", created_at=OLD, updated_at=OLD)
    await repository.create(name="touched", created_at=OLD, updated_at=NEWER)

    rows = await repository.list(_MixinModel.is_new)

    assert [row.name for row in rows] == ["new"]


# --- soft delete ---


@pytest.mark.usefixtures("tables")
@pytest.mark.parametrize(("tombstone", "expected"), [(False, True), (True, False)])
async def test_not_deleted_instance_level(repository, tombstone, expected):
    obj = await repository.create(name="john", tombstone=tombstone)

    assert obj.tombstone is tombstone
    assert obj.not_deleted is expected


@pytest.mark.usefixtures("tables")
async def test_tombstone_defaults_to_false(repository):
    obj = await repository.create(name="john")

    assert obj.tombstone is False
    assert obj.not_deleted is True


def test_not_deleted_sql_expression():
    assert str(_MixinModel.not_deleted.expression) == "NOT test_mixins_model.tombstone"


@pytest.mark.usefixtures("tables")
async def test_not_deleted_as_sql_filter(repository):
    await repository.create(name="alive")
    await repository.create(name="deleted", tombstone=True)

    rows = await repository.list(_MixinModel.not_deleted)

    assert [row.name for row in rows] == ["alive"]
