from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import StatementError

from sqlargon import Database
from sqlargon.types.json import (
    json_array_append,
    json_array_length,
    json_get,
    json_has_key,
    json_insert_key,
    json_keys,
    json_remove_key,
    json_replace_key,
    json_set_key,
    json_update,
)
from sqlargon.utils import json_loads

from .backends import Backend
from .models import Address, Document, DocumentRepository, ServerDefaults, User

WARSAW = timezone(timedelta(hours=2))


async def store(documents: DocumentRepository, **values: object) -> Document:
    await documents.insert(
        {"tags": [], "payload": {}, **values}, return_results=False
    ).execute()
    stored = await documents.first()
    assert stored is not None
    return stored


async def test_guid_round_trips_as_a_uuid(db: Database):
    user_id = uuid4()
    await db.execute(sa.insert(User).values(id=user_id, name="John"))
    stored = (await db.execute(sa.select(User.id))).scalar()
    assert stored == user_id
    assert isinstance(stored, UUID)


async def test_timestamp_round_trips_in_utc(db: Database):
    created = datetime(2024, 5, 17, 12, 30, tzinfo=WARSAW)
    await db.execute(
        sa.insert(User).values(name="John", created_at=created, updated_at=created)
    )
    stored: datetime = (await db.execute(sa.select(User.created_at))).scalar_one()
    assert stored == created
    assert stored.tzinfo == timezone.utc


async def test_timestamp_rejects_a_naive_datetime(db: Database):
    with pytest.raises(StatementError, match="must have a timezone"):
        await db.execute(
            sa.insert(User).values(name="John", created_at=datetime(2024, 5, 17))  # noqa: DTZ001
        )


async def test_now_server_default_is_timezone_aware(db: Database):
    before = datetime.now(tz=timezone.utc)
    await db.execute(sa.insert(User).values(name="John"))
    stored: datetime = (await db.execute(sa.select(User.created_at))).scalar_one()
    assert stored.tzinfo == timezone.utc
    assert stored >= before - timedelta(seconds=5)


@pytest.mark.usefixtures("needs_server_side_uuid")
async def test_server_side_uuid_defaults(db: Database, backend: Backend):
    await db.execute(sa.insert(ServerDefaults))
    row = (await db.execute(sa.select(ServerDefaults))).scalars().one()
    assert isinstance(row.id, UUID)
    assert row.id.version == 4
    assert isinstance(row.uuid_v7, UUID)
    # the SQLite generators share one random, v4 shaped implementation
    assert row.uuid_v7.version == (4 if backend.dialect == "sqlite" else 7)
    assert row.created_at.tzinfo == timezone.utc


async def test_json_round_trips_a_mapping(documents: DocumentRepository):
    payload = {"nested": {"count": 3}, "flag": True, "items": [1, 2]}
    stored = await store(documents, payload=payload)
    assert stored.payload == payload


async def test_json_round_trips_a_sequence(documents: DocumentRepository):
    stored = await store(documents, tags=["red", "green"])
    assert stored.tags == ["red", "green"]


async def test_pydantic_column_round_trips(documents: DocumentRepository):
    stored = await store(documents, address=Address(city="Warsaw", zip_code="00-001"))
    assert stored.address == Address(city="Warsaw", zip_code="00-001")


async def test_pydantic_column_accepts_none(documents: DocumentRepository):
    stored = await store(documents, address=None)
    assert stored.address is None


async def test_json_contains_matches_a_subset(documents: DocumentRepository):
    await store(documents, tags=["red", "green", "blue"])
    matched = await documents.select().where(Document.tags.contains(["red"])).all()
    assert len(matched) == 1


async def test_json_contains_rejects_a_missing_element(
    documents: DocumentRepository,
):
    await store(documents, tags=["red"])
    matched = await documents.select().where(Document.tags.contains(["black"])).all()
    assert matched == []


async def test_json_value_reads_an_object_key(
    documents: DocumentRepository, backend: Backend
):
    await store(documents, payload={"city": "Warsaw"})
    value = await documents.select(Document.payload.json_value("city")).scalar()
    # MySQL's JSON_EXTRACT returns the JSON representation, quotes included
    expected = '"Warsaw"' if backend.dialect == "mysql" else "Warsaw"
    assert value == expected


@pytest.mark.usefixtures("needs_json_key_operators")
async def test_json_has_all_keys(documents: DocumentRepository):
    await store(documents, payload={"city": "Warsaw", "zip": "00-001"})
    matched = await (
        documents.select().where(Document.payload.has_all_keys(["city", "zip"])).all()
    )
    assert len(matched) == 1


@pytest.mark.usefixtures("needs_json_key_operators")
async def test_json_has_any_key(documents: DocumentRepository):
    await store(documents, payload={"city": "Warsaw"})
    matched = await (
        documents.select().where(Document.payload.has_any_key(["city", "zip"])).all()
    )
    assert len(matched) == 1


@pytest.mark.usefixtures("needs_json_key_operators")
async def test_json_has_any_key_rejects_unknown_keys(documents: DocumentRepository):
    await store(documents, payload={"city": "Warsaw"})
    matched = await (
        documents.select().where(Document.payload.has_any_key(["country"])).all()
    )
    assert matched == []


async def mutate(documents: DocumentRepository, expression: object) -> Document:
    """Apply ``expression`` to every document and read the row back."""
    await documents.update({Document.payload: expression}).execute()
    stored = await documents.first()
    assert stored is not None
    return stored


async def test_json_set_key_adds_a_key(documents: DocumentRepository):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_set_key(Document.payload, "b", 2))
    assert stored.payload == {"a": 1, "b": 2}


async def test_json_set_key_overwrites_a_key(documents: DocumentRepository):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_set_key(Document.payload, "a", 9))
    assert stored.payload == {"a": 9}


async def test_json_set_key_stores_a_nested_document(documents: DocumentRepository):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_set_key(Document.payload, "b", {"x": [1, 2]}))
    # the value is a document, not the serialized text as a JSON string
    assert stored.payload == {"a": 1, "b": {"x": [1, 2]}}


async def test_json_update_merges_shallowly(documents: DocumentRepository):
    await store(documents, payload={"a": {"x": 1}, "b": 2})
    stored = await mutate(
        documents, json_update(Document.payload, {"a": {"y": 9}, "c": 3})
    )
    # "a" is replaced wholesale rather than merged into
    assert stored.payload == {"a": {"y": 9}, "b": 2, "c": 3}


async def test_json_update_with_no_keys_leaves_the_document(
    documents: DocumentRepository,
):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_update(Document.payload, {}))
    assert stored.payload == {"a": 1}


async def test_json_remove_key_drops_keys(documents: DocumentRepository):
    await store(documents, payload={"a": 1, "b": 2, "c": 3})
    stored = await mutate(documents, json_remove_key(Document.payload, "a", "c"))
    assert stored.payload == {"b": 2}


async def test_json_remove_key_ignores_a_missing_key(documents: DocumentRepository):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_remove_key(Document.payload, "nope"))
    assert stored.payload == {"a": 1}


async def test_json_insert_key_only_adds_a_missing_key(documents: DocumentRepository):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_insert_key(Document.payload, "b", 2))
    assert stored.payload == {"a": 1, "b": 2}


async def test_json_insert_key_leaves_an_existing_key(documents: DocumentRepository):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_insert_key(Document.payload, "a", 9))
    assert stored.payload == {"a": 1}


async def test_json_replace_key_only_updates_an_existing_key(
    documents: DocumentRepository,
):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_replace_key(Document.payload, "a", 9))
    assert stored.payload == {"a": 9}


async def test_json_replace_key_does_not_add_a_missing_key(
    documents: DocumentRepository,
):
    await store(documents, payload={"a": 1})
    stored = await mutate(documents, json_replace_key(Document.payload, "b", 2))
    assert stored.payload == {"a": 1}


async def test_json_mutations_compose_in_one_statement(documents: DocumentRepository):
    await store(documents, payload={"a": 1, "b": 2})
    stored = await mutate(
        documents, json_remove_key(json_update(Document.payload, {"c": 3}), "a")
    )
    assert stored.payload == {"b": 2, "c": 3}


async def test_json_array_append_appends_one_element(documents: DocumentRepository):
    await store(documents, tags=["red"])
    await documents.update(
        {Document.tags: json_array_append(Document.tags, "green")}
    ).execute()
    stored = await documents.first()
    assert stored is not None
    assert stored.tags == ["red", "green"]


async def test_json_array_append_nests_a_list(documents: DocumentRepository):
    await store(documents, tags=["red"])
    await documents.update(
        {Document.tags: json_array_append(Document.tags, ["a", "b"])}
    ).execute()
    stored = await documents.first()
    assert stored is not None
    # appended as one element rather than concatenated
    assert stored.tags == ["red", ["a", "b"]]


async def test_json_has_key_addresses_object_keys(documents: DocumentRepository):
    # the gap has_any_key / has_all_keys leave on sqlite, whose json_each
    # fallback matches values instead of keys
    await store(documents, payload={"a": "b"})
    assert await documents.select().where(json_has_key(Document.payload, "a")).all()
    assert not await documents.select().where(json_has_key(Document.payload, "b")).all()


async def test_json_get_reads_a_nested_document(documents: DocumentRepository):
    await store(documents, payload={"a": {"x": 1}})
    value = await documents.select(json_get(Document.payload, "a")).scalar()
    # sqlite and mysql hand back the JSON text, postgres a decoded document
    if isinstance(value, str):
        value = json_loads(value)
    assert value == {"x": 1}


async def test_json_array_length_counts_elements(documents: DocumentRepository):
    await store(documents, tags=["a", "b", "c"])
    assert await documents.select(json_array_length(Document.tags)).scalar() == 3


async def test_json_keys_lists_top_level_keys(documents: DocumentRepository):
    await store(documents, payload={"a": 1, "b": 2})
    keys = await documents.select(json_keys(Document.payload)).scalar()
    if isinstance(keys, str):
        keys = json_loads(keys)
    assert sorted(keys) == ["a", "b"]
