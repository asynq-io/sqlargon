import json
import os.path
from collections.abc import Callable
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError
from pydantic_core import PydanticCustomError
from sqlalchemy.pool import NullPool

from sqlargon.imports import ImportedType, import_from_string, import_string_validator
from sqlargon.settings import DatabaseSettings
from sqlargon.utils import json_dumps, json_loads

# Models defined at module level to avoid re-registration with --count=3


class _BareImportModel(BaseModel):
    obj: ImportedType


class _TypedImportModel(BaseModel):
    func: ImportedType[Callable[[Any], str]]


# --- import_from_string ---


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("sqlalchemy.pool:NullPool", NullPool),
        ("sqlargon.utils:json_dumps", json_dumps),
        ("os:path", os.path),
    ],
)
def test_import_from_string(path, expected):
    assert import_from_string(path) is expected


def test_import_from_string_missing_object():
    with pytest.raises(ImportError, match="os has no object nope"):
        import_from_string("os:nope")


def test_import_from_string_without_object_part():
    # nothing after ":" means there is no object to look up
    with pytest.raises(ImportError, match="os has no object"):
        import_from_string("os")


def test_import_from_string_missing_module():
    with pytest.raises(ModuleNotFoundError):
        import_from_string("sqlargon_does_not_exist:thing")


# --- import_string_validator ---


def test_import_string_validator_imports_string():
    assert import_string_validator("sqlalchemy.pool:NullPool") is NullPool


def test_import_string_validator_wraps_import_error():
    with pytest.raises(PydanticCustomError) as exc_info:
        import_string_validator("os:nope")

    assert exc_info.value.type == "import_error"
    assert "os has no object nope" in str(exc_info.value)


@pytest.mark.parametrize("value", [42, None, {"a": 1}, NullPool, json_dumps])
def test_import_string_validator_passes_non_strings_through(value):
    assert import_string_validator(value) is value


# --- ImportedType as a pydantic annotation ---


def test_bare_imported_type_validates_import_string():
    assert _BareImportModel(obj="sqlalchemy.pool:NullPool").obj is NullPool


def test_bare_imported_type_passes_non_string_through():
    assert _BareImportModel(obj=42).obj == 42


def test_bare_imported_type_rejects_invalid_path():
    with pytest.raises(ValidationError, match="Invalid python path"):
        _BareImportModel(obj="os:nope")


def test_typed_imported_type_validates_import_string():
    assert _TypedImportModel(func="sqlargon.utils:json_dumps").func is json_dumps


def test_typed_imported_type_validates_imported_object_against_schema():
    # os.sep imports fine but is not a callable
    with pytest.raises(ValidationError, match="callable"):
        _TypedImportModel(func="os:sep")


def test_imported_type_repr():
    metadata = _TypedImportModel.model_fields["func"].metadata
    assert [repr(m) for m in metadata] == ["ImportedType"]


# --- serialization ---


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # objects carrying __module__ and __name__
        ("sqlalchemy.pool:NullPool", "sqlalchemy.pool.impl:NullPool"),
        ("sqlargon.utils:json_dumps", "sqlargon.utils:json_dumps"),
        # a module serializes to its name
        ("os:path", "posixpath"),
        # anything else is returned unchanged
        (42, 42),
    ],
)
def test_imported_type_json_serialization(value, expected):
    dumped = json.loads(_BareImportModel(obj=value).model_dump_json())
    assert dumped == {"obj": expected}


def test_imported_type_python_dump_is_not_serialized():
    # the serializer is registered with when_used="json"
    assert _BareImportModel(obj="sqlalchemy.pool:NullPool").model_dump() == {
        "obj": NullPool
    }


# --- DatabaseSettings integration ---


def test_settings_imports_poolclass():
    settings = DatabaseSettings(poolclass="sqlalchemy.pool:NullPool")
    assert settings.poolclass is NullPool


def test_settings_accepts_poolclass_object():
    settings = DatabaseSettings(poolclass=NullPool)
    assert settings.poolclass is NullPool


def test_settings_rejects_invalid_poolclass():
    with pytest.raises(ValidationError, match="Invalid python path"):
        DatabaseSettings(poolclass="sqlalchemy.pool:NoSuchPool")


def test_settings_default_json_hooks():
    settings = DatabaseSettings()
    assert settings.json_serializer is json_dumps
    assert settings.json_deserializer is json_loads


def test_settings_json_round_trip():
    settings = DatabaseSettings(poolclass="sqlalchemy.pool:NullPool")

    dumped = json.loads(settings.model_dump_json())

    assert dumped["poolclass"] == "sqlalchemy.pool.impl:NullPool"
    assert dumped["json_serializer"] == "sqlargon.utils:json_dumps"
    assert dumped["json_deserializer"] == "sqlargon.utils:json_loads"
    assert DatabaseSettings(**dumped).poolclass is NullPool
