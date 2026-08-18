from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import StatementError

from sqlargon import Database

from .backends import Backend
from .models import (
    Address,
    Document,
    DocumentRepository,
    ServerDefaults,
    ServerDefaultsUUIDV7,
    User,
)

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
async def test_server_side_uuid_defaults(db: Database):
    await db.execute(sa.insert(ServerDefaults))
    row = (await db.execute(sa.select(ServerDefaults))).scalars().one()
    assert isinstance(row.id, UUID)
    assert row.id.version == 4
    assert row.created_at.tzinfo == timezone.utc


@pytest.mark.usefixtures("needs_server_side_uuidv7")
async def test_server_side_uuidv7_default(db: Database, backend: Backend):
    await db.execute(sa.insert(ServerDefaultsUUIDV7))
    row = (await db.execute(sa.select(ServerDefaultsUUIDV7))).scalars().one()
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
