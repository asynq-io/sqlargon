from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.engine.default import DefaultDialect

from sqlargon.orm import Base as _Base
from sqlargon.types import GUID, JSON, GenerateUUID, GenerateUUIDV7, Timestamp, now
from sqlargon.types.json import (
    json_contains,
    json_has_all_keys,
    json_has_any_key,
    json_value,
)
from sqlargon.types.pydantic import Pydantic, ValidatedType


def _compile(expr, dialect, *, literal_binds=True) -> str:
    """Compile an expression against ``dialect`` without executing it."""
    return str(
        expr.compile(dialect=dialect, compile_kwargs={"literal_binds": literal_binds})
    )


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


# --- GenerateUUID / GenerateUUIDV7 ---


@pytest.mark.parametrize(
    ("element", "expected"),
    [
        (GenerateUUID, "GEN_RANDOM_UUID()"),
        (GenerateUUIDV7, "uuidv7()"),
    ],
)
def test_generate_uuid_postgresql(element, expected):
    assert _compile(sa.select(element()), postgresql.dialect()).startswith(
        f"SELECT {expected}"
    )


@pytest.mark.parametrize(
    ("element", "expected", "not_expected"),
    [
        (GenerateUUID, "'-4'", "UNIX_TIMESTAMP"),
        (GenerateUUIDV7, "'-7'", "HEX(RANDOM_BYTES(4))"),
    ],
)
def test_generate_uuid_mysql(element, expected, not_expected):
    sql = _compile(sa.select(element()), mysql.dialect())
    assert "LOWER(CONCAT(" in sql
    assert "HEX(RANDOM_BYTES(6))" in sql
    assert expected in sql
    assert not_expected not in sql


@pytest.mark.parametrize("element", [GenerateUUID, GenerateUUIDV7])
def test_generate_uuid_sqlite(element):
    sql = _compile(sa.select(element()), sqlite.dialect())
    assert "randomblob(4)" in sql
    assert "randomblob(6)" in sql


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


def test_timestamp_dialect_impl_mysql():
    impl = Timestamp().load_dialect_impl(mysql.dialect())
    assert isinstance(impl, mysql.DATETIME)
    assert impl.fsp == 6


def test_timestamp_dialect_impl_fallback():
    impl = Timestamp().load_dialect_impl(DefaultDialect())
    assert isinstance(impl, sa.TIMESTAMP)
    assert impl.timezone is True


def test_timestamp_bind_mysql_converts_to_utc():
    est = timezone(timedelta(hours=-5))
    value = datetime(2024, 1, 1, 12, 0, 0, tzinfo=est)
    result = Timestamp().process_bind_param(value, mysql.dialect())
    assert result == datetime(2024, 1, 1, 17, 0, 0, tzinfo=timezone.utc)


# --- now() ---


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (mysql.dialect(), "SELECT CURRENT_TIMESTAMP(6) AS now_1"),
        (sqlite.dialect(), "SELECT strftime('%Y-%m-%d %H:%M:%f000', 'now') AS now_1"),
        (postgresql.dialect(), "SELECT CURRENT_TIMESTAMP AS now_1"),
        (DefaultDialect(), "SELECT CURRENT_TIMESTAMP AS now_1"),
    ],
)
def test_now_compiles_per_dialect(dialect, expected):
    assert _compile(sa.select(now()), dialect) == expected


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


@pytest.mark.parametrize("dialect", [mysql.dialect(), DefaultDialect()])
def test_json_dialect_impl_fallback(dialect):
    impl = JSON().load_dialect_impl(dialect)
    assert isinstance(impl, sa.JSON)
    assert not isinstance(impl, (postgresql.JSONB, sqlite.JSON))
    assert impl.none_as_null is True


# --- JSON per-dialect compilation ---

_json_col = sa.column("data", JSON())


def test_json_contains_compiles_postgresql():
    sql = _compile(
        json_contains(_json_col, ["a", "b"]),
        postgresql.dialect(),
        literal_binds=False,
    )
    assert sql == "data @> %(param_1)s::JSONB"


def test_json_contains_compiles_mysql():
    sql = _compile(
        json_contains(_json_col, ["a", "b"]), mysql.dialect(), literal_binds=False
    )
    assert sql == "json_contains(data, %s)"


@pytest.mark.parametrize(
    ("left", "right", "expected_left", "expected_right"),
    [
        (["a", "b"], ["a"], '["a","b"]', '["a"]'),
        ({"a": 1}, {"a": 1}, '{"a":1}', '{"a":1}'),
        (("a", "b"), ("a",), '["a","b"]', '["a"]'),
        ("abc", "abc", '"abc"', '"abc"'),
    ],
)
def test_json_contains_sqlite_serializes_python_values(
    left, right, expected_left, expected_right
):
    compiled = json_contains(left, right).compile(dialect=sqlite.dialect())
    sql = str(compiled)
    assert 'json_each(?) AS "left"' in sql
    assert 'json_each(?) AS "right"' in sql
    assert compiled.params["json_each_1"] == expected_left
    assert compiled.params["json_each_2"] == expected_right


@pytest.mark.parametrize(
    ("factory", "operator"),
    [(json_has_any_key, "?|"), (json_has_all_keys, "?&")],
)
def test_json_has_keys_compiles_postgresql(factory, operator):
    sql = _compile(factory(_json_col, ["a", "b"]), postgresql.dialect())
    assert sql == f"data {operator} ARRAY['a', 'b']"


@pytest.mark.parametrize(
    ("factory", "operator"),
    [(json_has_any_key, "?|"), (json_has_all_keys, "?&")],
)
def test_json_has_keys_empty_values_casts_array_postgresql(factory, operator):
    sql = _compile(factory(_json_col, []), postgresql.dialect())
    assert sql == f"data {operator} CAST(ARRAY[] AS VARCHAR[])"


@pytest.mark.parametrize(
    ("factory", "mode"),
    [(json_has_any_key, "one"), (json_has_all_keys, "all")],
)
def test_json_has_keys_compiles_mysql(factory, mode):
    sql = _compile(factory(_json_col, ["a", "b"]), mysql.dialect())
    assert sql == f"json_contains_path(data, '{mode}', '$.a', '$.b')"


@pytest.mark.parametrize(
    ("factory", "expected"),
    [(json_has_any_key, "false"), (json_has_all_keys, "true")],
)
def test_json_has_keys_empty_values_short_circuits_mysql(factory, expected):
    assert _compile(factory(_json_col, []), mysql.dialect()) == expected


def test_json_has_any_key_compiles_sqlite():
    sql = _compile(
        json_has_any_key(_json_col, ["a", "b"]), sqlite.dialect(), literal_binds=False
    )
    assert "EXISTS (SELECT 1" in sql
    assert "json_each(data) AS json_each" in sql
    assert "json_each.value IN (__[POSTCOMPILE_json_each_values_1])" in sql


def test_json_has_all_keys_compiles_sqlite_as_containment():
    compiled = json_has_all_keys(_json_col, ["a", "b"]).compile(
        dialect=sqlite.dialect()
    )
    sql = str(compiled)
    assert 'json_each(data) AS "left"' in sql
    assert "count(DISTINCT left.value)" in sql
    assert compiled.params["json_each_1"] == '["a","b"]'


def test_json_value_init():
    element = json_value(_json_col, "key")
    assert element.column is _json_col
    assert element.key == "key"
    assert element.name == "json_value"
    assert isinstance(element.type, sa.String)
    assert list(element.clauses) == [_json_col]


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (postgresql.dialect(), "data ->> 'k'"),
        (mysql.dialect(), "json_extract(data, '$.\"k\"')"),
        (sqlite.dialect(), "json_extract(data, '$.\"k\"')"),
        (DefaultDialect(), "json_extract(data, '$.\"k\"')"),
    ],
)
def test_json_value_compiles_per_dialect(dialect, expected):
    assert _compile(json_value(_json_col, "k"), dialect) == expected


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


class _JsonValueModel(_Base):
    __tablename__ = "test_json_value"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    data = sa.Column(JSON())


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


async def test_json_value_sqlite(db):
    async with db.engine.begin() as conn:
        await conn.run_sync(_JsonValueModel.__table__.create, checkfirst=True)
    try:
        async with db.session() as session:
            session.add(_JsonValueModel(data={"k": "v"}))
            session.add(_JsonValueModel(data={"k": "other"}))

        async with db.session() as session:
            result = await session.execute(
                sa.select(_JsonValueModel).where(
                    _JsonValueModel.data.json_value("k") == "v"
                )
            )
            rows = result.scalars().all()
            assert len(rows) == 1
            assert rows[0].data == {"k": "v"}
    finally:
        async with db.engine.begin() as conn:
            await conn.run_sync(_JsonValueModel.__table__.drop, checkfirst=True)


# --- JSON comparator compilation ---


def test_json_comparator_contains_postgresql():
    sql = _compile(
        sa.select(_JsonValueModel.id).where(_JsonValueModel.data.contains(["a"])),
        postgresql.dialect(),
        literal_binds=False,
    )
    assert "WHERE test_json_value.data @> %(param_1)s::JSONB" in sql


@pytest.mark.parametrize(
    ("method", "operator"),
    [("has_any_key", "?|"), ("has_all_keys", "?&")],
)
def test_json_comparator_has_keys_postgresql(method, operator):
    expr = getattr(_JsonValueModel.data, method)(["a", "b"])
    sql = _compile(sa.select(_JsonValueModel.id).where(expr), postgresql.dialect())
    assert f"WHERE test_json_value.data {operator} ARRAY['a', 'b']" in sql


@pytest.mark.parametrize(
    ("method", "mode"),
    [("has_any_key", "one"), ("has_all_keys", "all")],
)
def test_json_comparator_has_keys_mysql(method, mode):
    expr = getattr(_JsonValueModel.data, method)(["a", "b"])
    sql = _compile(sa.select(_JsonValueModel.id).where(expr), mysql.dialect())
    assert (
        f"WHERE json_contains_path(test_json_value.data, '{mode}', '$.a', '$.b')" in sql
    )


def test_json_comparator_json_value_postgresql():
    sql = _compile(
        sa.select(_JsonValueModel.id).where(
            _JsonValueModel.data.json_value("k") == "v"
        ),
        postgresql.dialect(),
    )
    assert "WHERE test_json_value.data ->> 'k' = 'v'" in sql


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
