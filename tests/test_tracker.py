import pytest
import sqlalchemy as sa

from sqlargon import Database
from sqlargon.tracker import TRACKER, ConnectionTracker


@pytest.fixture(autouse=True)
def tracker():
    # TRACKER is a module level singleton shared by every engine created with
    # enable_tracker=True, so its state must be reset around each test.
    TRACKER.clear()
    yield TRACKER
    TRACKER.clear()


@pytest.fixture
async def tracked_database():
    db = Database("sqlite+aiosqlite:///:memory:", enable_tracker=True)
    try:
        yield db
    finally:
        await db.dispose()


def test_tracker_is_singleton_and_starts_empty(tracker):
    assert isinstance(tracker, ConnectionTracker)
    assert tracker.connects == 0
    assert tracker.closes == 0
    assert tracker.open_connections == {}
    assert tracker.left_field_closes == {}


async def test_track_pool_records_connect(tracker, tracked_database):
    async with tracked_database.session() as session:
        assert (await session.execute(sa.text("SELECT 1"))).scalar() == 1

    assert tracker.connects == 1
    assert tracker.closes == 0
    assert len(tracker.open_connections) == 1
    assert tracker.left_field_closes == {}

    stack = next(iter(tracker.open_connections.values()))
    assert stack
    assert all(isinstance(line, str) for line in stack)


async def test_track_pool_records_close_on_dispose(tracker, tracked_database):
    async with tracked_database.session() as session:
        await session.execute(sa.text("SELECT 1"))

    assert len(tracker.open_connections) == 1

    await tracked_database.dispose()

    assert tracker.closes == 1
    assert tracker.open_connections == {}
    assert tracker.left_field_closes == {}


async def test_database_without_tracker_is_not_tracked(tracker, db: Database):
    async with db.session() as session:
        await session.execute(sa.text("SELECT 1"))

    assert tracker.connects == 0
    assert tracker.open_connections == {}


def test_on_connect_records_connection(tracker):
    connection = object()

    tracker.on_connect(connection, None)

    assert tracker.connects == 1
    assert tracker.closes == 0
    assert connection in tracker.open_connections
    assert tracker.open_connections[connection]


@pytest.mark.parametrize(
    ("method_name", "extra_args"),
    [("on_close", (None,)), ("on_close_detached", ())],
)
def test_close_removes_open_connection(tracker, method_name, extra_args):
    connection = object()
    tracker.on_connect(connection, None)

    getattr(tracker, method_name)(connection, *extra_args)

    assert tracker.closes == 1
    assert tracker.open_connections == {}
    assert tracker.left_field_closes == {}


@pytest.mark.parametrize(
    ("method_name", "extra_args"),
    [("on_close", (None,)), ("on_close_detached", ())],
)
def test_close_of_untracked_connection_is_left_field(tracker, method_name, extra_args):
    connection = object()

    getattr(tracker, method_name)(connection, *extra_args)

    assert tracker.closes == 1
    assert tracker.connects == 0
    assert connection in tracker.left_field_closes
    assert tracker.left_field_closes[connection]
    assert tracker.open_connections == {}


def test_clear_resets_all_state(tracker):
    tracker.on_connect(object(), None)
    tracker.on_close(object(), None)
    assert tracker.open_connections
    assert tracker.left_field_closes

    tracker.clear()

    assert tracker.connects == 0
    assert tracker.closes == 0
    assert tracker.open_connections == {}
    assert tracker.left_field_closes == {}
