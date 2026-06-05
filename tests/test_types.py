from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.dialects import postgresql, sqlite

from sqlargon.orm import Base as _Base
from sqlargon.types import GUID, JSON, Timestamp
from sqlargon.types.json import json_has_all_keys, json_has_any_key
from sqlargon.types.pydantic import Pydantic, ValidatedType

# --- GUID ---


def test_guid_bind_uuid_sqlite():
    guid = GUID()
    test_uuid = uuid4()
    result = guid.process_bind_param(test_uuid, sqlite.dialect())
    assert isinstance(result, str)
    assert result == str(test_uuid)


def test_guid_bind_string_sqlite():
    guid = GUID()
    test_uuid = uuid4()
    result = guid.process_bind_param(str(test_uuid), sqlite.dialect())
    assert result == str(test_uuid)


def test_guid_bind_none():
    assert GUID().process_bind_param(None, sqlite.dialect()) is None


def test_guid_bind_uuid_postgresql():
    guid = GUID()
    test_uuid = uuid4()
    result = guid.process_bind_param(test_uuid, postgresql.dialect())
    assert result == test_uuid


def test_guid_bind_string_postgresql():
    guid = GUID()
    test_uuid = uuid4()
    result = guid.process_bind_param(str(test_uuid), postgresql.dialect())
    assert isinstance(result, UUID)
    assert result == test_uuid


def test_guid_result_string():
    guid = GUID()
    test_uuid = uuid4()
    result = guid.process_result_value(str(test_uuid), sqlite.dialect())
    assert isinstance(result, UUID)
    assert result == test_uuid


def test_guid_result_uuid():
    guid = GUID()
    test_uuid = uuid4()
    result = guid.process_result_value(test_uuid, sqlite.dialect())
    assert result == test_uuid


def test_guid_result_none():
    assert GUID().process_result_value(None, sqlite.dialect()) is None


def test_guid_dialect_impl_sqlite():
    assert GUID().load_dialect_impl(sqlite.dialect()) is not None


def test_guid_dialect_impl_postgresql():
    assert GUID().load_dialect_impl(postgresql.dialect()) is not None


# --- Timestamp ---


def test_timestamp_bind_sqlite():
    ts = Timestamp()
    value = datetime.now(tz=timezone.utc)
    result = ts.process_bind_param(value, sqlite.dialect())
    assert result.tzinfo == timezone.utc


def test_timestamp_bind_none():
    assert Timestamp().process_bind_param(None, sqlite.dialect()) is None


def test_timestamp_bind_no_tz_raises():
    naive_dt = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    with pytest.raises(ValueError, match="Timestamps must have a timezone"):
        Timestamp().process_bind_param(naive_dt, sqlite.dialect())


def test_timestamp_bind_sqlite_converts_non_utc_to_utc():

    est = timezone(timedelta(hours=-5))
    value = datetime(2024, 1, 1, 12, 0, 0, tzinfo=est)
    result = Timestamp().process_bind_param(value, sqlite.dialect())
    assert result == datetime(2024, 1, 1, 17, 0, 0, tzinfo=timezone.utc)


def test_timestamp_bind_postgresql():
    ts = Timestamp()
    value = datetime.now(tz=timezone.utc)
    result = ts.process_bind_param(value, postgresql.dialect())
    assert result is value


def test_timestamp_result():
    ts = Timestamp()
    naive_dt = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    result = ts.process_result_value(naive_dt, sqlite.dialect())
    assert result.tzinfo == timezone.utc


def test_timestamp_result_none():
    assert Timestamp().process_result_value(None, sqlite.dialect()) is None


def test_timestamp_dialect_impl_sqlite():
    assert Timestamp().load_dialect_impl(sqlite.dialect()) is not None


def test_timestamp_dialect_impl_postgresql():
    assert Timestamp().load_dialect_impl(postgresql.dialect()) is not None


# --- JSON ---


def test_json_dialect_impl_sqlite():
    assert JSON().load_dialect_impl(sqlite.dialect()) is not None


def test_json_dialect_impl_postgresql():
    assert JSON().load_dialect_impl(postgresql.dialect()) is not None


def test_json_has_any_key_requires_strings():
    with pytest.raises(ValueError, match="must be strings"):
        json_has_any_key(sa.literal(1), [1, 2, 3])


def test_json_has_all_keys_requires_strings():
    with pytest.raises(ValueError, match="must be strings"):
        json_has_all_keys(sa.literal(1), [1, 2, 3])


# --- JSON integration (SQLite) ---
# Models defined at module level to avoid re-registration with --count=3


class _JsonCrudModel(_Base):
    __tablename__ = "test_json_crud"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    data = sa.Column(JSON())


class _JsonContainsModel(_Base):
    __tablename__ = "test_json_contains"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    tags = sa.Column(JSON())


class _JsonHasAnyModel(_Base):
    __tablename__ = "test_json_has_any"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    tags = sa.Column(JSON())


class _JsonHasAllModel(_Base):
    __tablename__ = "test_json_has_all"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    tags = sa.Column(JSON())


async def test_json_column_crud(db):
    async with db.engine.begin() as conn:
        await conn.run_sync(_JsonCrudModel.__table__.create, checkfirst=True)
    try:
        async with db.session() as session:
            session.add(_JsonCrudModel(data={"key": "value", "items": [1, 2]}))

        async with db.session() as session:
            result = await session.execute(sa.select(_JsonCrudModel))
            row = result.scalars().first()
            assert row.data == {"key": "value", "items": [1, 2]}
    finally:
        async with db.engine.begin() as conn:
            await conn.run_sync(_JsonCrudModel.__table__.drop, checkfirst=True)


async def test_json_contains_sqlite(db):
    async with db.engine.begin() as conn:
        await conn.run_sync(_JsonContainsModel.__table__.create, checkfirst=True)
    try:
        async with db.session() as session:
            session.add(_JsonContainsModel(tags=["python", "sql", "async"]))
            session.add(_JsonContainsModel(tags=["java", "spring"]))

        async with db.session() as session:
            result = await session.execute(
                sa.select(_JsonContainsModel).where(
                    _JsonContainsModel.tags.contains(["python", "sql"])
                )
            )
            rows = result.scalars().all()
            assert len(rows) == 1
    finally:
        async with db.engine.begin() as conn:
            await conn.run_sync(_JsonContainsModel.__table__.drop, checkfirst=True)


async def test_json_has_any_key_sqlite(db):
    async with db.engine.begin() as conn:
        await conn.run_sync(_JsonHasAnyModel.__table__.create, checkfirst=True)
    try:
        async with db.session() as session:
            session.add(_JsonHasAnyModel(tags=["a", "b", "c"]))
            session.add(_JsonHasAnyModel(tags=["d", "e"]))

        async with db.session() as session:
            result = await session.execute(
                sa.select(_JsonHasAnyModel).where(
                    _JsonHasAnyModel.tags.has_any_key(["a", "x"])
                )
            )
            rows = result.scalars().all()
            assert len(rows) == 1
    finally:
        async with db.engine.begin() as conn:
            await conn.run_sync(_JsonHasAnyModel.__table__.drop, checkfirst=True)


async def test_json_has_all_keys_sqlite(db):
    async with db.engine.begin() as conn:
        await conn.run_sync(_JsonHasAllModel.__table__.create, checkfirst=True)
    try:
        async with db.session() as session:
            session.add(_JsonHasAllModel(tags=["a", "b", "c"]))
            session.add(_JsonHasAllModel(tags=["a"]))

        async with db.session() as session:
            result = await session.execute(
                sa.select(_JsonHasAllModel).where(
                    _JsonHasAllModel.tags.has_all_keys(["a", "b"])
                )
            )
            rows = result.scalars().all()
            assert len(rows) == 1
            assert rows[0].tags == ["a", "b", "c"]
    finally:
        async with db.engine.begin() as conn:
            await conn.run_sync(_JsonHasAllModel.__table__.drop, checkfirst=True)


# --- Pydantic ---


class SampleModel(BaseModel):
    x: int
    y: str


def test_pydantic_bind_model():
    ptype = Pydantic(SampleModel)
    model = SampleModel(x=1, y="test")
    result = ptype.process_bind_param(model, sqlite.dialect())
    assert result == {"x": 1, "y": "test"}


def test_pydantic_bind_dict():
    ptype = Pydantic(SampleModel)
    result = ptype.process_bind_param({"x": 1, "y": "test"}, sqlite.dialect())
    assert result == {"x": 1, "y": "test"}


def test_pydantic_bind_none():
    assert Pydantic(SampleModel).process_bind_param(None, sqlite.dialect()) is None


def test_pydantic_result():
    ptype = Pydantic(SampleModel)
    result = ptype.process_result_value({"x": 1, "y": "test"}, sqlite.dialect())
    assert isinstance(result, SampleModel)
    assert result.x == 1
    assert result.y == "test"


def test_pydantic_result_none():
    assert Pydantic(SampleModel).process_result_value(None, sqlite.dialect()) is None


def test_pydantic_custom_sa_column_type():
    ptype = Pydantic(SampleModel, sa_column_type=sa.JSON)
    assert ptype.impl == sa.JSON


# --- ValidatedType ---


def test_validated_type_bind():
    vtype = ValidatedType(list[int])
    result = vtype.process_bind_param([1, 2, 3], sqlite.dialect())
    assert result == [1, 2, 3]


def test_validated_type_bind_none():
    assert ValidatedType(list[int]).process_bind_param(None, sqlite.dialect()) is None


def test_validated_type_result():
    vtype = ValidatedType(list[int])
    result = vtype.process_result_value([1, 2, 3], sqlite.dialect())
    assert result == [1, 2, 3]


def test_validated_type_result_none():
    assert ValidatedType(list[int]).process_result_value(None, sqlite.dialect()) is None


def test_validated_type_no_validation():
    vtype = ValidatedType(list[int], validate=False)
    result = vtype.process_bind_param(["not", "ints"], sqlite.dialect())
    assert result == ["not", "ints"]


def test_validated_type_custom_sa_column_type():
    vtype = ValidatedType(list[int], sa_column_type=sa.JSON)
    assert vtype.impl == sa.JSON
