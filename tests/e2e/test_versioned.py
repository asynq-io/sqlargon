"""E2E tests for VersionedRepository across real database backends.

UUID-based versioning runs on every backend; ``xmin`` tests are scoped to
PostgreSQL via the ``needs_xmin`` fixture.
"""

from __future__ import annotations

import pytest

from sqlargon import ConcurrentModificationError

from .models import VersionedUser, XminUser

NAMES = ("Andrew", "John", "Vincent")


async def seed(versioned_users, *names: str) -> None:
    await versioned_users.bulk_create(
        [{"name": name} for name in names], return_results=False
    )


# --- UUID versioning (all backends) ---


async def test_create_sets_initial_version(versioned_users):
    user = await versioned_users.create(name="John")

    assert user is not None
    assert user.version_id is not None


async def test_update_increments_version(versioned_users):
    user = await versioned_users.create(name="John")
    original = user.version_id

    updated = await versioned_users.update_one(
        {"name": "Jane"}, VersionedUser.id == user.id
    )

    assert updated is not None
    assert updated.version_id != original


async def test_update_if_match_matching_version(versioned_users):
    user = await versioned_users.create(name="John")

    updated = await versioned_users.update_if_match(
        {"name": "Jane"},
        VersionedUser.id == user.id,
        expected_version=user.version_id,
    )

    assert updated is not None
    assert updated.name == "Jane"


async def test_update_if_match_mismatched_version(versioned_users):
    user = await versioned_users.create(name="John")
    from uuid_utils.compat import uuid4 as _uuid4  # noqa: PLC0415

    updated = await versioned_users.update_if_match(
        {"name": "Jane"},
        VersionedUser.id == user.id,
        expected_version=_uuid4(),
    )

    assert updated is None
    # the row is unchanged
    stored = await versioned_users.get(id=user.id)
    assert stored is not None
    assert stored.name == "John"


async def test_update_if_match_raises_on_mismatch(versioned_users):
    user = await versioned_users.create(name="John")
    from uuid_utils.compat import uuid4 as _uuid4  # noqa: PLC0415

    with pytest.raises(ConcurrentModificationError, match="was modified or deleted"):
        await versioned_users.update_if_match(
            {"name": "Jane"},
            VersionedUser.id == user.id,
            expected_version=_uuid4(),
            raise_on_mismatch=True,
        )


async def test_delete_if_match_matching_version(versioned_users):
    user = await versioned_users.create(name="John")

    deleted = await versioned_users.delete_if_match(
        VersionedUser.id == user.id,
        expected_version=user.version_id,
    )

    assert deleted is not None
    assert deleted.name == "John"
    assert await versioned_users.count() == 0


async def test_delete_if_match_mismatched_version(versioned_users):
    user = await versioned_users.create(name="John")
    from uuid_utils.compat import uuid4 as _uuid4  # noqa: PLC0415

    deleted = await versioned_users.delete_if_match(
        VersionedUser.id == user.id,
        expected_version=_uuid4(),
    )

    assert deleted is None
    assert await versioned_users.count() == 1


async def test_upsert_preserves_version(versioned_users):
    user = await versioned_users.create(name="John")
    original = user.version_id

    await versioned_users.create_or_update(id=user.id, name="Johnny")

    stored = await versioned_users.get(id=user.id)
    assert stored is not None
    assert stored.name == "Johnny"
    assert stored.version_id == original


async def test_bulk_update_increments_version(versioned_users):
    await seed(versioned_users, *NAMES)
    rows = await versioned_users.list()
    original = {row.id: row.version_id for row in rows}

    await versioned_users.bulk_update(
        [{"id": row.id, "name": f"renamed {row.name}"} for row in rows]
    )

    updated = {row.id: row.version_id for row in await versioned_users.list()}
    assert all(updated[rid] != original[rid] for rid in original)


# --- xmin versioning (PostgreSQL only) ---


@pytest.mark.usefixtures("needs_xmin")
async def test_xmin_changes_on_update(xmin_users):
    user = await xmin_users.create(name="John")
    original = user.xmin

    updated = await xmin_users.update_one({"name": "Jane"}, XminUser.id == user.id)

    assert updated is not None
    assert updated.xmin != original


@pytest.mark.usefixtures("needs_xmin")
async def test_xmin_update_if_match_matching(xmin_users):
    user = await xmin_users.create(name="John")

    updated = await xmin_users.update_if_match(
        {"name": "Jane"},
        XminUser.id == user.id,
        expected_version=user.xmin,
    )

    assert updated is not None
    assert updated.name == "Jane"


@pytest.mark.usefixtures("needs_xmin")
async def test_xmin_update_if_match_mismatched(xmin_users):
    user = await xmin_users.create(name="John")

    updated = await xmin_users.update_if_match(
        {"name": "Jane"},
        XminUser.id == user.id,
        expected_version="999999999",
    )

    assert updated is None


@pytest.mark.usefixtures("needs_xmin")
async def test_xmin_delete_if_match_matching(xmin_users):
    user = await xmin_users.create(name="John")

    deleted = await xmin_users.delete_if_match(
        XminUser.id == user.id,
        expected_version=user.xmin,
    )

    assert deleted is not None
    assert deleted.name == "John"
    assert await xmin_users.count() == 0
