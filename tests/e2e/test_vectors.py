"""Vector search against a real server.

Runs on the backends that can search vectors: PostgreSQL through pgvector,
SQLite through the sqlite-vector loadable extension. The two take entirely
different paths -- an ORDER BY over a distance operator against a join over a
table valued scan -- so every similarity test is worth running on both.

Full text search and reciprocal rank fusion are PostgreSQL only.
"""

from typing import TYPE_CHECKING

import pytest
import sqlalchemy as sa

from sqlargon import Database
from sqlargon.vectors import (
    DistanceMetric,
    UnsupportedDialectError,
    VectorCollectionRepository,
)

from .backends import Backend
from .models import VectorDoc, VectorDocRepository, VectorNoteRepository

if TYPE_CHECKING:
    from collections.abc import Sequence

pytestmark = pytest.mark.usefixtures("needs_vector_search")

# unit vectors along the three axes, so cosine distances are exactly known
X = [1.0, 0.0, 0.0]
Y = [0.0, 1.0, 0.0]
Z = [0.0, 0.0, 1.0]

DOCUMENTS = [
    {"text": "red apple fruit", "embedding": X, "attributes": {"lang": "en"}},
    {"text": "blue sky above", "embedding": Y, "attributes": {"lang": "en"}},
    {"text": "green apple tree", "embedding": Z, "attributes": {"lang": "de"}},
]


@pytest.fixture
async def documents(vector_docs: VectorDocRepository) -> VectorDocRepository:
    await vector_docs.create_many(DOCUMENTS)
    return vector_docs


def _texts(models: "Sequence[VectorDoc]") -> list[str]:
    return [model.text for model in models]


async def test_embedding_round_trips_as_floats(vector_docs: VectorDocRepository):
    await vector_docs.create(text="one", embedding=[0.25, 0.5, 0.75])
    stored = await vector_docs.one()
    assert stored.embedding == pytest.approx([0.25, 0.5, 0.75])


@pytest.mark.parametrize(
    ("query", "expected"),
    [(X, "red apple fruit"), (Y, "blue sky above"), (Z, "green apple tree")],
)
async def test_search_returns_the_nearest_first(
    documents: VectorDocRepository, query, expected
):
    found = await documents.search(query, limit=1)
    assert _texts(found) == [expected]


async def test_search_orders_every_row_by_distance(documents: VectorDocRepository):
    found = await documents.search([1.0, 0.1, 0.0], limit=3)
    assert _texts(found)[0] == "red apple fruit"
    assert _texts(found)[1] == "blue sky above"


async def test_search_honours_the_limit(documents: VectorDocRepository):
    assert len(await documents.search(X, limit=2)) == 2


async def test_search_reports_distances(documents: VectorDocRepository):
    found = await documents.search(X, limit=2, with_distance=True)
    nearest, distance = found[0]
    assert nearest.text == "red apple fruit"
    # cosine distance of a vector to itself
    assert distance == pytest.approx(0.0, abs=1e-5)
    assert found[1][1] > distance


async def test_hybrid_search_filters_out_the_nearest_neighbour(
    documents: VectorDocRepository,
):
    """The filter has to run before the limit, not after it.

    ``X`` is the embedding of the English row, so a scan that took its top
    hit first and filtered afterwards would come back empty.
    """
    found = await documents.search(
        X, VectorDoc.attributes_contain({"lang": "de"}), limit=2
    )
    assert _texts(found) == ["green apple tree"]


async def test_hybrid_search_accepts_arbitrary_expressions(
    documents: VectorDocRepository,
):
    found = await documents.search(X, VectorDoc.text.like("%tree%"), limit=2)
    assert _texts(found) == ["green apple tree"]


async def test_hybrid_search_accepts_keyword_equalities(
    documents: VectorDocRepository,
):
    found = await documents.search(X, collection_id=None, limit=3)
    assert len(found) == 3


async def test_search_works_without_the_optional_columns(
    vector_notes: VectorNoteRepository,
):
    """A model carrying nothing but an embedding still searches."""
    await vector_notes.create_many(
        [
            {"name": "x-axis", "embedding": X},
            {"name": "z-axis", "embedding": Z},
        ]
    )
    found = await vector_notes.search(Z, limit=1)
    assert [note.name for note in found] == ["z-axis"]


async def test_search_by_collection(
    documents: VectorDocRepository, vector_db: Database
):
    collections = VectorCollectionRepository().using(db=vector_db)
    collection = await collections.create(name="library")
    assert collection is not None
    await documents.update_one({"collection_id": collection.id}, text="blue sky above")
    found = await documents.search(X, collection_id=collection.id, limit=3)
    assert _texts(found) == ["blue sky above"]


# --- PostgreSQL only ---


@pytest.fixture
def needs_postgresql(backend: Backend) -> None:
    if backend.dialect != "postgresql":
        pytest.skip(f"{backend.name} has no PostgreSQL full text search")


@pytest.mark.usefixtures("needs_postgresql")
async def test_text_search_ranks_matches(documents: VectorDocRepository):
    found = await documents.text_search("apple", limit=3)
    assert sorted(_texts(found)) == ["green apple tree", "red apple fruit"]


@pytest.mark.usefixtures("needs_postgresql")
async def test_text_search_reports_scores(documents: VectorDocRepository):
    found = await documents.text_search("apple", limit=3, with_score=True)
    assert all(score > 0 for _, score in found)


@pytest.mark.usefixtures("needs_postgresql")
async def test_text_search_ignores_non_matches(documents: VectorDocRepository):
    assert await documents.text_search("submarine", limit=3) == []


@pytest.mark.usefixtures("needs_postgresql")
async def test_rrf_search_prefers_what_both_rankings_agree_on(
    documents: VectorDocRepository,
):
    """The row both rankings find outranks the rows only one of them does.

    ``Z`` is the embedding of "green apple tree" and it is the only row
    matching "tree", so it takes the top of both rankings while the other
    two appear in the vector ranking alone. Searching for "apple" instead
    would leave the top two tied, since ``ts_rank`` scores both rows
    matching it the same.
    """
    found = await documents.rrf_search(Z, "tree", limit=3)
    assert found[0][0].text == "green apple tree"
    assert found[0][1] > found[1][1]


@pytest.mark.usefixtures("needs_postgresql")
async def test_rrf_search_keeps_rows_only_one_ranking_found(
    documents: VectorDocRepository,
):
    found = await documents.rrf_search(Y, "apple", limit=3)
    assert set(_texts([model for model, _ in found])) == {
        "red apple fruit",
        "blue sky above",
        "green apple tree",
    }


@pytest.mark.usefixtures("needs_postgresql")
async def test_metric_override_changes_the_ordering(
    documents: VectorDocRepository,
):
    found = await documents.search(X, limit=1, metric=DistanceMetric.L2)
    assert _texts(found) == ["red apple fruit"]


@pytest.mark.usefixtures("needs_postgresql")
async def test_the_indexes_exist(vector_db: Database):
    result = await vector_db.execute(
        sa.text("SELECT indexdef FROM pg_indexes WHERE tablename = 'e2e_vector_doc'")
    )
    definitions = " ".join(row[0] for row in result)
    assert "USING hnsw" in definitions
    assert "vector_cosine_ops" in definitions
    assert "USING gin" in definitions
    assert "to_tsvector" in definitions


# --- SQLite only ---


@pytest.fixture
def needs_sqlite(backend: Backend) -> None:
    if backend.dialect != "sqlite":
        pytest.skip(f"{backend.name} is not sqlite")


@pytest.mark.usefixtures("needs_sqlite")
async def test_search_survives_a_fresh_pooled_connection(
    documents: VectorDocRepository, vector_db: Database
):
    """The extension is loaded per connection, so a new one must get it too."""
    await vector_db.dispose()
    found = await documents.search(X, limit=1)
    assert _texts(found) == ["red apple fruit"]


@pytest.mark.usefixtures("needs_sqlite")
async def test_repeated_searches_reuse_the_initialised_column(
    documents: VectorDocRepository,
):
    assert _texts(await documents.search(X, limit=1)) == ["red apple fruit"]
    assert _texts(await documents.search(Z, limit=1)) == ["green apple tree"]


@pytest.mark.usefixtures("needs_sqlite")
async def test_text_search_is_rejected(documents: VectorDocRepository):
    with pytest.raises(UnsupportedDialectError, match="text_search"):
        await documents.text_search("apple")


@pytest.mark.usefixtures("needs_sqlite")
async def test_metric_override_is_rejected(documents: VectorDocRepository):
    with pytest.raises(UnsupportedDialectError, match="per column"):
        await documents.search(X, metric=DistanceMetric.L2)
