from contextlib import asynccontextmanager
from typing import Annotated

import httpx
import pytest
import sqlalchemy as sa
from fastapi import Depends, FastAPI
from sqlalchemy.ext.asyncio import AsyncSession

from sqlargon import (
    Base,
    Database,
    DatabaseCluster,
    SQLAlchemyRepository,
    SQLAlchemyUnitOfWork,
    get_default_database,
)
from sqlargon.integrations.fastapi import (
    DatabaseMiddleware,
    Db,
    Provide,
    Session,
    transaction,
)
from sqlargon.types import GUID, GenerateUUID

MEMORY_URL = "sqlite+aiosqlite:///:memory:"


class Customer(Base):
    __tablename__ = "customer"

    id = sa.Column(
        GUID(), primary_key=True, server_default=GenerateUUID(), nullable=False
    )
    name = sa.Column(sa.Unicode(255))


class CustomerRepository(SQLAlchemyRepository[Customer]):
    default_order_by = Customer.name.desc()


class CustomerUow(SQLAlchemyUnitOfWork):
    customers: CustomerRepository


@pytest.fixture
async def customers(db: Database):
    await db.create_all()
    yield CustomerRepository()
    await db.drop_all()


async def cycle_lifespan(app) -> list[str]:
    """Drive the ASGI lifespan protocol, returning the messages sent back."""
    incoming = ["lifespan.startup", "lifespan.shutdown"]
    sent = []

    async def receive():
        return {"type": incoming.pop(0)}

    async def send(message):
        sent.append(message["type"])

    await app({"type": "lifespan"}, receive, send)
    return sent


@pytest.fixture
def client_factory():
    @asynccontextmanager
    async def factory(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            yield client

    return factory


@pytest.fixture
def own_db():
    return Database(MEMORY_URL)


async def test_middleware_sets_default_database_before_lifespan(own_db):
    seen = []

    @asynccontextmanager
    async def lifespan(_app):
        seen.append(get_default_database())
        yield

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(DatabaseMiddleware, database=own_db)

    sent = await cycle_lifespan(app)

    assert seen == [own_db]
    assert sent == ["lifespan.startup.complete", "lifespan.shutdown.complete"]


async def test_middleware_clears_default_database_on_shutdown(own_db):
    app = FastAPI()
    app.add_middleware(DatabaseMiddleware, database=own_db)

    await cycle_lifespan(app)

    assert get_default_database() is not own_db


@pytest.mark.parametrize("dispose", [True, False])
async def test_middleware_disposes_according_to_flag(own_db, monkeypatch, dispose):
    disposed = []

    async def dispose_engine():
        disposed.append(True)

    monkeypatch.setattr(own_db, "dispose", dispose_engine)

    app = FastAPI()
    app.add_middleware(DatabaseMiddleware, database=own_db, dispose=dispose)
    await cycle_lifespan(app)

    assert bool(disposed) is dispose


@pytest.mark.parametrize("verify", [True, False])
async def test_middleware_verifies_according_to_flag(own_db, monkeypatch, verify):
    verified = []

    async def verify_connection():
        verified.append(True)

    monkeypatch.setattr(own_db, "verify_connection", verify_connection)

    app = FastAPI()
    app.add_middleware(DatabaseMiddleware, database=own_db, verify=verify)
    await cycle_lifespan(app)

    assert bool(verified) is verify


async def test_middleware_verify_failure_fails_startup(own_db, monkeypatch):
    started = []

    async def verify_connection():
        msg = "unreachable"
        raise RuntimeError(msg)

    monkeypatch.setattr(own_db, "verify_connection", verify_connection)

    @asynccontextmanager
    async def lifespan(_app):
        started.append(True)
        yield

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(DatabaseMiddleware, database=own_db)

    with pytest.raises(RuntimeError, match="unreachable"):
        await cycle_lifespan(app)

    assert started == []


async def test_middleware_disposes_when_startup_fails(own_db, monkeypatch):
    disposed = []

    async def dispose_engine():
        disposed.append(True)

    async def verify_connection():
        msg = "unreachable"
        raise RuntimeError(msg)

    monkeypatch.setattr(own_db, "dispose", dispose_engine)
    monkeypatch.setattr(own_db, "verify_connection", verify_connection)

    app = FastAPI()
    app.add_middleware(DatabaseMiddleware, database=own_db)

    with pytest.raises(RuntimeError, match="unreachable"):
        await cycle_lifespan(app)

    assert disposed
    assert get_default_database() is not own_db


async def test_middleware_disposes_when_app_startup_fails(own_db, monkeypatch):
    disposed = []

    async def dispose_engine():
        disposed.append(True)

    monkeypatch.setattr(own_db, "dispose", dispose_engine)

    @asynccontextmanager
    async def lifespan(_app):
        msg = "app failed to start"
        raise RuntimeError(msg)
        yield

    app = FastAPI(lifespan=lifespan)
    app.add_middleware(DatabaseMiddleware, database=own_db)

    with pytest.raises(RuntimeError, match="app failed to start"):
        await cycle_lifespan(app)

    assert disposed
    assert get_default_database() is not own_db


async def test_middleware_falls_back_to_default_database(db, monkeypatch):
    verified = []

    async def verify_connection():
        verified.append(True)

    monkeypatch.setattr(db, "verify_connection", verify_connection)

    app = FastAPI()
    app.add_middleware(DatabaseMiddleware, dispose=False)
    await cycle_lifespan(app)

    assert verified == [True]
    assert get_default_database() is db


async def test_middleware_passes_requests_through(client_factory, own_db):
    app = FastAPI()
    app.add_middleware(DatabaseMiddleware, database=own_db)

    @app.get("/ping")
    async def ping():
        return {"ping": "pong"}

    async with client_factory(app) as client:
        response = await client.get("/ping")

    assert response.json() == {"ping": "pong"}


@pytest.mark.usefixtures("customers")
async def test_provide_injects_repository(client_factory):
    app = FastAPI()

    @app.post("/customers")
    async def create_customer(name: str, repository: Provide[CustomerRepository]):
        customer = await repository.create(name=name)
        assert customer is not None
        return {"name": customer.name}

    @app.get("/customers")
    async def list_customers(repository: Provide[CustomerRepository]):
        return [{"name": customer.name} for customer in await repository.all()]

    async with client_factory(app) as client:
        await client.post("/customers", params={"name": "John"})
        response = await client.get("/customers")

    assert response.json() == [{"name": "John"}]


async def test_provide_adds_no_request_parameters(client_factory):
    app = FastAPI()

    @app.get("/customers")
    async def list_customers(repository: Provide[CustomerRepository]):
        return len(await repository.all())

    async with client_factory(app) as client:
        schema = (await client.get("/openapi.json")).json()

    assert "parameters" not in schema["paths"]["/customers"]["get"]


@pytest.mark.usefixtures("customers")
async def test_provide_injects_unit_of_work(client_factory):
    app = FastAPI()

    @app.post("/customers")
    async def create_customer(name: str, uow: Provide[CustomerUow]):
        async with uow:
            customer = await uow.customers.create(name=name)
            assert customer is not None
            return {"name": customer.name}

    async with client_factory(app) as client:
        response = await client.post("/customers", params={"name": "John"})

    assert response.json() == {"name": "John"}


async def test_db_injects_default_database(client_factory, db):
    app = FastAPI()

    @app.get("/dialect")
    async def dialect(database: Db):
        assert database is db
        return {"dialect": database.dialect}

    async with client_factory(app) as client:
        response = await client.get("/dialect")

    assert response.json() == {"dialect": "sqlite"}


async def test_session_injects_request_scoped_session(client_factory, db):
    app = FastAPI()

    @app.get("/session")
    async def current_session(session: Session):
        assert session is db._current_session.get()
        return {"ok": True}

    async with client_factory(app) as client:
        response = await client.get("/session")

    assert response.json() == {"ok": True}


@pytest.mark.usefixtures("customers")
async def test_transaction_shares_one_session(client_factory, db: Database):
    app = FastAPI()

    @app.post("/customers", dependencies=[Depends(transaction())])
    async def create_customer(name: str, repository: Provide[CustomerRepository]):
        customer = await repository.create(name=name)
        assert customer is not None
        session = db._current_session.get()
        assert session is not None
        found = await session.get(Customer, customer.id)
        return {"found": found is not None}

    async with client_factory(app) as client:
        response = await client.post("/customers", params={"name": "John"})

    assert response.json() == {"found": True}


@pytest.mark.usefixtures("customers")
@pytest.mark.parametrize("in_transaction", [True, False])
async def test_transaction_rolls_back_on_error(client_factory, in_transaction):
    app = FastAPI()
    dependencies = [Depends(transaction())] if in_transaction else []

    @app.post("/customers", dependencies=dependencies)
    async def create_customer(name: str, repository: Provide[CustomerRepository]):
        await repository.create(name=name)
        msg = "boom"
        raise RuntimeError(msg)

    @app.get("/customers")
    async def list_customers(repository: Provide[CustomerRepository]):
        return len(await repository.all())

    async with client_factory(app) as client:
        with pytest.raises(RuntimeError, match="boom"):
            await client.post("/customers", params={"name": "John"})
        remaining = (await client.get("/customers")).json()

    assert remaining == (0 if in_transaction else 1)


async def test_transaction_routes_to_hinted_database(client_factory):
    primary = Database(MEMORY_URL)
    other = Database(MEMORY_URL)
    cluster = DatabaseCluster({"primary": primary, "other": other})
    app = FastAPI()

    @app.get("/routed")
    async def routed(
        session: Annotated[
            AsyncSession, Depends(transaction("other", database=cluster))
        ],
    ):
        return {"routed": session.get_bind() is other.engine.sync_engine}

    async with client_factory(app) as client:
        response = await client.get("/routed")

    assert response.json() == {"routed": True}
