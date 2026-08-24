import contextlib
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
    json_array_append,
    json_array_length,
    json_contains,
    json_get,
    json_has_all_keys,
    json_has_any_key,
    json_has_key,
    json_insert_key,
    json_keys,
    json_remove_key,
    json_replace_key,
    json_set_key,
    json_update,
    json_value,
)
from sqlargon.types.pydantic import Pydantic, ValidatedType
from sqlargon.utils import json_loads


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


# --- JSON literal rendering ---


@pytest.mark.parametrize(
    "dialect", [postgresql.dialect(), sqlite.dialect(), mysql.dialect()]
)
def test_json_literal_processor_renders_null(dialect):
    assert JSON().literal_processor(dialect)(None) == "NULL"


@pytest.mark.parametrize(
    "dialect", [postgresql.dialect(), sqlite.dialect(), mysql.dialect()]
)
def test_json_literal_processor_serializes_and_quotes_a_document(dialect):
    assert JSON().literal_processor(dialect)({"a": 1}) == "'{\"a\":1}'"


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
    # the key and its JSON path are operands, not compile time literals, so
    # that one cached statement can serve every key
    column, key, path = element.clauses
    assert column is _json_col
    assert key.value == "key"
    assert path.value == '$."key"'


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


# --- JSON mutations and reads, per-dialect compilation ---


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (postgresql.dialect(), """(data || CAST('{"a":1}' AS JSONB))"""),
        (sqlite.dialect(), """json_set(data, '$."a"', json('1'))"""),
        (mysql.dialect(), """json_set(data, '$."a"', json_extract('1', '$'))"""),
        (DefaultDialect(), """json_set(data, '$."a"', json_extract('1', '$'))"""),
    ],
)
def test_json_update_compiles_per_dialect(dialect, expected):
    assert _compile(json_update(_json_col, {"a": 1}), dialect) == expected


def test_json_set_key_is_a_single_key_update():
    assert _compile(json_set_key(_json_col, "a", 1), sqlite.dialect()) == _compile(
        json_update(_json_col, {"a": 1}), sqlite.dialect()
    )


@pytest.mark.parametrize(
    "dialect", [sqlite.dialect(), mysql.dialect(), DefaultDialect()]
)
def test_json_update_merges_every_key_in_one_call(dialect):
    sql = _compile(json_update(_json_col, {"a": 1, "b": 2}), dialect)
    # one json_set, not a nested pair -- and shallow, so a nested object
    # replaces rather than merges
    assert sql.count("json_set(") == 1
    assert """'$."a"'""" in sql
    assert """'$."b"'""" in sql


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (postgresql.dialect(), "(data - CAST(ARRAY['a', 'b'] AS TEXT[]))"),
        (sqlite.dialect(), """json_remove(data, '$."a"', '$."b"')"""),
        (mysql.dialect(), """json_remove(data, '$."a"', '$."b"')"""),
        (DefaultDialect(), """json_remove(data, '$."a"', '$."b"')"""),
    ],
)
def test_json_remove_key_compiles_per_dialect(dialect, expected):
    assert _compile(json_remove_key(_json_col, "a", "b"), dialect) == expected


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (postgresql.dialect(), """(CAST('{"a":1}' AS JSONB) || data)"""),
        (sqlite.dialect(), """json_insert(data, '$."a"', json('1'))"""),
        (mysql.dialect(), """json_insert(data, '$."a"', json_extract('1', '$'))"""),
        (DefaultDialect(), """json_insert(data, '$."a"', json_extract('1', '$'))"""),
    ],
)
def test_json_insert_key_compiles_per_dialect(dialect, expected):
    # the patch goes on the left of the postgres concat so an existing key wins
    assert _compile(json_insert_key(_json_col, "a", 1), dialect) == expected


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (
            postgresql.dialect(),
            "jsonb_set(data, CAST(ARRAY['a'] AS TEXT[]), CAST('1' AS JSONB), false)",
        ),
        (sqlite.dialect(), """json_replace(data, '$."a"', json('1'))"""),
        (mysql.dialect(), """json_replace(data, '$."a"', json_extract('1', '$'))"""),
        (DefaultDialect(), """json_replace(data, '$."a"', json_extract('1', '$'))"""),
    ],
)
def test_json_replace_key_compiles_per_dialect(dialect, expected):
    # create_missing => false is what keeps postgres from inserting the key
    assert _compile(json_replace_key(_json_col, "a", 1), dialect) == expected


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (
            postgresql.dialect(),
            """(data || jsonb_build_array(CAST('"x"' AS JSONB)))""",
        ),
        (sqlite.dialect(), """json_insert(data, '$[#]', json('"x"'))"""),
        (
            mysql.dialect(),
            """json_array_append(data, '$', json_extract('"x"', '$'))""",
        ),
        (
            DefaultDialect(),
            """json_array_append(data, '$', json_extract('"x"', '$'))""",
        ),
    ],
)
def test_json_array_append_compiles_per_dialect(dialect, expected):
    assert _compile(json_array_append(_json_col, "x"), dialect) == expected


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (postgresql.dialect(), "(data -> 'k')"),
        (sqlite.dialect(), """json_extract(data, '$."k"')"""),
        (mysql.dialect(), """json_extract(data, '$."k"')"""),
        (DefaultDialect(), """json_extract(data, '$."k"')"""),
    ],
)
def test_json_get_compiles_per_dialect(dialect, expected):
    assert _compile(json_get(_json_col, "k"), dialect) == expected


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (postgresql.dialect(), "data ? 'k'"),
        (sqlite.dialect(), """json_type(data, '$."k"') IS NOT NULL"""),
        (mysql.dialect(), """json_contains_path(data, 'one', '$."k"')"""),
        (DefaultDialect(), """json_contains_path(data, 'one', '$."k"')"""),
    ],
)
def test_json_has_key_compiles_per_dialect(dialect, expected):
    assert _compile(json_has_key(_json_col, "k"), dialect) == expected


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (postgresql.dialect(), "jsonb_array_length(data)"),
        (sqlite.dialect(), "json_array_length(data)"),
        (mysql.dialect(), "json_length(data)"),
        (DefaultDialect(), "json_length(data)"),
    ],
)
def test_json_array_length_compiles_per_dialect(dialect, expected):
    assert _compile(json_array_length(_json_col), dialect) == expected


def test_json_keys_compiles_per_dialect():
    assert _compile(json_keys(_json_col), mysql.dialect()) == "json_keys(data)"
    assert _compile(json_keys(_json_col), DefaultDialect()) == "json_keys(data)"

    postgres = _compile(json_keys(_json_col), postgresql.dialect())
    assert "jsonb_object_keys(data)" in postgres
    # jsonb_agg over an object with no keys is NULL, not an empty array
    assert "coalesce" in postgres
    assert "CAST('[]' AS JSONB)" in postgres

    assert "json_group_array(json_each.key)" in _compile(
        json_keys(_json_col), sqlite.dialect()
    )


@pytest.mark.parametrize(
    "dialect",
    [postgresql.dialect(), sqlite.dialect(), mysql.dialect(), DefaultDialect()],
)
def test_json_update_with_no_keys_is_the_column(dialect):
    # json_set(col) with no pair is a syntax error, and merging nothing is
    # the column itself
    assert _compile(json_update(_json_col, {}), dialect) == "data"


@pytest.mark.parametrize(
    "dialect",
    [postgresql.dialect(), sqlite.dialect(), mysql.dialect(), DefaultDialect()],
)
def test_json_remove_key_with_no_keys_is_the_column(dialect):
    assert _compile(json_remove_key(_json_col), dialect) == "data"


@pytest.mark.parametrize(
    ("factory", "message"),
    [(json_update, "json_update keys"), (json_remove_key, "json_remove_key keys")],
)
def test_json_mutation_keys_must_be_strings(factory, message):
    argument = {1: "a"} if factory is json_update else 1
    with pytest.raises(ValueError, match=message):
        factory(_json_col, argument)


def test_json_mutations_nest():
    expression = json_remove_key(json_update(_json_col, {"a": 1}), "b")
    assert (
        _compile(expression, sqlite.dialect())
        == """json_remove(json_set(data, '$."a"', json('1')), '$."b"')"""
    )


def test_json_mutations_nest_on_postgresql_without_losing_precedence():
    # binary "-" binds tighter than "||" in postgres, so an unparenthesized
    # "a || b - c" would drop the key from the patch, not from the result
    expression = json_remove_key(json_update(_json_col, {"a": 1}), "b")
    assert (
        _compile(expression, postgresql.dialect())
        == """((data || CAST('{"a":1}' AS JSONB)) - CAST(ARRAY['b'] AS TEXT[]))"""
    )


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("plain", '$."plain"'),
        ('we"ird', '$."we\\"ird"'),
        ("back\\slash", '$."back\\\\slash"'),
    ],
)
def test_json_path_escapes_the_key(key, expected):
    # the path is an operand, so assert the value we hand the driver; how it
    # is then quoted into SQL text differs per dialect and is SQLAlchemy's job
    _column, _mapping, path, _value = json_set_key(_json_col, key, 1).clauses
    assert path.value == expected


def test_json_mutation_values_are_bound_not_inlined():
    compiled = json_update(_json_col, {"a": {"nested": True}}).compile(
        dialect=sqlite.dialect()
    )
    assert {"nested": True} in compiled.params.values()


def test_json_mutation_values_serialize_to_a_json_document():
    # json() / json_extract() re-parse this text, so the value lands as a
    # document rather than as a JSON string holding the serialized text.
    # A bare dialect serializes with the stdlib, an engine with orjson, so
    # compare the document and not the spacing.
    serialized = JSON().bind_processor(sqlite.dialect())({"nested": True})
    assert json_loads(serialized) == {"nested": True}


# the comparator is the documented entry point, so every method has to
# resolve through an InstrumentedAttribute and compile
_COMPARATOR_CALLS = [
    ("set_key", lambda c: c.set_key("a", 1), True),
    ("update", lambda c: c.update({"a": 1}), True),
    ("remove_key", lambda c: c.remove_key("a"), True),
    ("insert_key", lambda c: c.insert_key("a", 1), True),
    ("replace_key", lambda c: c.replace_key("a", 1), True),
    ("array_append", lambda c: c.array_append(1), True),
    ("get", lambda c: c.get("a"), True),
    ("keys", lambda c: c.keys(), True),
    # these answer with a boolean, an int and text, so they are ends of a
    # chain rather than links in one
    ("has_key", lambda c: c.has_key("a"), False),
    ("array_length", lambda c: c.array_length(), False),
    ("json_value", lambda c: c.json_value("a"), False),
    ("contains", lambda c: c.contains(["a"]), False),
]


_COMPARATOR_CASES = [
    (call, returns_json) for _name, call, returns_json in _COMPARATOR_CALLS
]
_COMPARATOR_IDS = [name for name, _call, _returns_json in _COMPARATOR_CALLS]


@pytest.mark.parametrize(
    ("call", "returns_json"), _COMPARATOR_CASES, ids=_COMPARATOR_IDS
)
def test_json_comparator_methods_resolve_and_compile(call, returns_json):
    expression = call(_JsonMutationModel.data)
    assert _compile(expression, sqlite.dialect())
    # a JSON-typed result carries the comparator again, which is what lets
    # the mutations chain: col.set_key(...).remove_key(...)
    assert isinstance(expression.type, JSON) is returns_json


@pytest.mark.parametrize(
    ("call", "returns_json"), _COMPARATOR_CASES, ids=_COMPARATOR_IDS
)
def test_json_comparator_methods_chain_when_they_return_json(call, returns_json):
    expression = call(_JsonMutationModel.data)
    assert hasattr(expression, "remove_key") is returns_json


@pytest.mark.parametrize(
    "factory",
    [
        lambda key: json_update(_json_col, {key: 1}),
        lambda key: json_remove_key(_json_col, key),
        lambda key: json_get(_json_col, key),
        lambda key: json_has_key(_json_col, key),
        lambda key: json_value(_json_col, key),
    ],
)
def test_json_keys_are_bound_so_a_cached_statement_serves_any_key(factory):
    # the keys live in the clause list, so two expressions share one compiled
    # statement and each execution binds its own key. A key baked in by a
    # @compiles hook would instead be reused for every later key.
    first, second = sa.select(factory("a")), sa.select(factory("b"))
    assert first._generate_cache_key() == second._generate_cache_key()
    assert str(first.compile(dialect=sqlite.dialect())) == str(
        second.compile(dialect=sqlite.dialect())
    )


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


class _JsonMutationModel(_Base):
    __tablename__ = "test_json_mutation"
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


# --- JSON mutations (SQLite) ---


@contextlib.asynccontextmanager
async def _mutation_table(db, initial):
    """The mutation table holding one row of ``initial``."""
    async with db.engine.begin() as conn:
        await conn.run_sync(_JsonMutationModel.__table__.create, checkfirst=True)
    try:
        async with db.session() as session:
            session.add(_JsonMutationModel(id=1, data=initial))
        yield
    finally:
        async with db.engine.begin() as conn:
            await conn.run_sync(_JsonMutationModel.__table__.drop, checkfirst=True)


async def _mutate(db, expression):
    """Apply ``expression`` to the row's data column and read it back."""
    async with db.session() as session:
        await session.execute(
            sa.update(_JsonMutationModel).values({_JsonMutationModel.data: expression})
        )
        await session.commit()
    async with db.session() as session:
        return await session.scalar(sa.select(_JsonMutationModel.data))


async def test_json_set_key_adds_a_key(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_set_key(column, "b", 2)) == {"a": 1, "b": 2}


async def test_json_set_key_overwrites_a_key(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_set_key(column, "a", 9)) == {"a": 9}


async def test_json_set_key_stores_a_nested_document(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        # not the serialized text as a JSON string
        assert await _mutate(db, json_set_key(column, "b", {"x": [1, 2]})) == {
            "a": 1,
            "b": {"x": [1, 2]},
        }


async def test_json_update_merges_shallowly(db):
    async with _mutation_table(db, {"a": {"x": 1}, "b": 2}):
        column = _JsonMutationModel.data
        # "a" is replaced wholesale rather than merged into
        assert await _mutate(db, json_update(column, {"a": {"y": 9}, "c": 3})) == {
            "a": {"y": 9},
            "b": 2,
            "c": 3,
        }


async def test_json_update_with_no_keys_leaves_the_document(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_update(column, {})) == {"a": 1}


async def test_json_remove_key_drops_keys(db):
    async with _mutation_table(db, {"a": 1, "b": 2, "c": 3}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_remove_key(column, "a", "c")) == {"b": 2}


async def test_json_remove_key_ignores_a_missing_key(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_remove_key(column, "nope")) == {"a": 1}


async def test_json_insert_key_only_adds_a_missing_key(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_insert_key(column, "b", 2)) == {"a": 1, "b": 2}


async def test_json_insert_key_leaves_an_existing_key(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_insert_key(column, "a", 9)) == {"a": 1}


async def test_json_replace_key_only_updates_an_existing_key(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_replace_key(column, "a", 9)) == {"a": 9}


async def test_json_replace_key_does_not_add_a_missing_key(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_replace_key(column, "b", 2)) == {"a": 1}


async def test_json_array_append_appends_one_element(db):
    async with _mutation_table(db, [1, 2]):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_array_append(column, 3)) == [1, 2, 3]


async def test_json_array_append_nests_a_list_rather_than_concatenating(db):
    async with _mutation_table(db, [1]):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_array_append(column, [2, 3])) == [1, [2, 3]]


async def test_json_mutations_compose_in_one_statement(db):
    async with _mutation_table(db, {"a": 1, "b": 2}):
        column = _JsonMutationModel.data
        expression = json_remove_key(json_update(column, {"c": 3}), "a")
        assert await _mutate(db, expression) == {"b": 2, "c": 3}


async def test_json_mutation_of_a_null_column_propagates_null(db):
    async with _mutation_table(db, None):
        column = _JsonMutationModel.data
        assert await _mutate(db, json_set_key(column, "a", 1)) is None


async def test_json_reads_on_sqlite(db):
    async with _mutation_table(db, {"a": {"x": 1}, "b": [1, 2, 3]}):
        column = _JsonMutationModel.data
        async with db.session() as session:
            assert await session.scalar(sa.select(json_get(column, "a"))) == {"x": 1}
            assert await session.scalar(sa.select(json_has_key(column, "a")))
            assert not await session.scalar(sa.select(json_has_key(column, "nope")))
            assert (
                await session.scalar(
                    sa.select(json_array_length(json_get(column, "b")))
                )
                == 3
            )
            assert sorted(await session.scalar(sa.select(json_keys(column)))) == [
                "a",
                "b",
            ]


async def test_json_has_key_addresses_object_keys_not_values(db):
    # the gap json_has_any_key / json_has_all_keys leave on sqlite, where
    # their json_each fallback matches values instead
    async with _mutation_table(db, {"a": "b"}):
        column = _JsonMutationModel.data
        async with db.session() as session:
            assert await session.scalar(sa.select(json_has_key(column, "a")))
            assert not await session.scalar(sa.select(json_has_key(column, "b")))


async def test_json_comparator_methods_reach_the_mutations(db):
    async with _mutation_table(db, {"a": 1}):
        column = _JsonMutationModel.data
        assert await _mutate(db, column.set_key("b", 2)) == {"a": 1, "b": 2}
        assert await _mutate(db, column.remove_key("a")) == {"b": 2}
        assert await _mutate(db, column.update({"c": 3})) == {"b": 2, "c": 3}
