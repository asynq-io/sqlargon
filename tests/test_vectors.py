import struct
import sys

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.orm import declared_attr

from sqlargon import Base, Database, SQLAlchemyRepository
from sqlargon.dialects.mysql import MysqlQueryBuilder
from sqlargon.dialects.postgres import PostgresqlQueryBuilder
from sqlargon.dialects.sqlite import SQLiteQueryBuilder
from sqlargon.mixins import UUIDV7ModelMixin
from sqlargon.query_builder import Option, QueryBuilder
from sqlargon.types.vector import (
    cosine_distance,
    distance_for,
    l1_distance,
    l2_distance,
    max_inner_product,
)
from sqlargon.vectors import (
    AttributesMixin,
    DistanceMetric,
    EmbeddingBase,
    EmbeddingMixin,
    HybridVectorRepository,
    TextBase,
    TextEmbeddingBase,
    TextMixin,
    UnsupportedDialectError,
    Vector,
    VectorCollection,
    VectorCollectionRepository,
    VectorDocument,
    VectorRepository,
    init_vectors,
    register_sqlite_vector,
)
from tests import MEMORY_URL

# Models defined at module level to avoid re-registration with --count=3

VECTOR = [1.0, 2.0, 3.0]


class Note(UUIDV7ModelMixin, EmbeddingBase):
    """An embedding and nothing else -- the minimum the extension supports."""

    __tablename__ = "test_vectors_note"
    __vector_dim__ = 3

    @declared_attr.directive
    def __table_args__(cls) -> tuple[sa.Index, ...]:
        return (cls.embedding_index(),)


class Article(UUIDV7ModelMixin, TextBase):
    """Text without an embedding."""

    __tablename__ = "test_vectors_article"
    __text_regconfig__ = "english"

    @declared_attr.directive
    def __table_args__(cls) -> tuple[sa.Index, ...]:
        return (cls.text_index(),)


class Chunk(UUIDV7ModelMixin, AttributesMixin, EmbeddingBase):
    """Embedding plus attributes, composed from the mixins."""

    __tablename__ = "test_vectors_chunk"
    __vector_dim__ = 4
    __vector_distance__ = DistanceMetric.L2

    @declared_attr.directive
    def __table_args__(cls) -> tuple[sa.Index, ...]:
        return (cls.embedding_index(), cls.attributes_index())


class Document(VectorDocument):
    """Everything: embedding, text, attributes and a collection."""

    __tablename__ = "test_vectors_document"
    __vector_dim__ = 3

    @declared_attr.directive
    def __table_args__(cls) -> tuple[sa.Index, ...]:
        return (cls.embedding_index(), cls.attributes_index(), cls.text_index())


class Plain(Base):
    __tablename__ = "test_vectors_plain"
    id = sa.Column(sa.Integer, primary_key=True)


class Composite(EmbeddingMixin, Base):
    """A composite primary key, which the rank fusion cannot key rows by."""

    __tablename__ = "test_vectors_composite"
    __vector_dim__ = 3
    left = sa.Column(sa.Integer, primary_key=True)
    right = sa.Column(sa.Integer, primary_key=True)


class NoteRepository(VectorRepository[Note]):
    pass


class ArticleRepository(SQLAlchemyRepository[Article]):
    pass


class ChunkRepository(VectorRepository[Chunk]):
    pass


class DocumentRepository(HybridVectorRepository[Document]):
    pass


class CompositeRepository(VectorRepository[Composite]):
    pass


def _compile(expr, dialect, *, literal_binds=True) -> str:
    """Compile an expression against ``dialect`` without executing it."""
    return str(
        expr.compile(dialect=dialect, compile_kwargs={"literal_binds": literal_binds})
    )


# --- Vector type ---


@pytest.mark.parametrize(
    ("dialect", "expected"),
    [
        (postgresql.dialect(), "VECTOR(3)"),
        (sqlite.dialect(), "BLOB"),
        (mysql.dialect(), "JSON"),
    ],
)
def test_vector_column_type_per_dialect(dialect, expected):
    assert expected in _compile(
        sa.schema.CreateColumn(Note.__table__.c.embedding), dialect
    )


def test_vector_dimensions_are_configurable():
    assert "VECTOR(4)" in _compile(
        sa.schema.CreateColumn(Chunk.__table__.c.embedding), postgresql.dialect()
    )


def test_vector_sqlite_round_trip():
    vector = Vector(3)
    packed = vector.process_bind_param(VECTOR, sqlite.dialect())
    assert packed == struct.pack("<3f", *VECTOR)
    assert vector.process_result_value(packed, sqlite.dialect()) == VECTOR


def test_vector_postgresql_bind_passes_the_list_through():
    assert Vector(3).process_bind_param(VECTOR, postgresql.dialect()) == VECTOR


def test_vector_normalises_non_list_results():
    assert (
        Vector(3).process_result_value((1.0, 2.0, 3.0), postgresql.dialect()) == VECTOR
    )


@pytest.mark.parametrize("dialect", [postgresql.dialect(), sqlite.dialect()])
def test_vector_none_stays_none(dialect):
    vector = Vector(3)
    assert vector.process_bind_param(None, dialect) is None
    assert vector.process_result_value(None, dialect) is None


# --- distance expressions ---


@pytest.mark.parametrize(
    ("metric", "operator"),
    [
        (DistanceMetric.COSINE, "<=>"),
        (DistanceMetric.L2, "<->"),
        (DistanceMetric.DOT, "<#>"),
        (DistanceMetric.L1, "<+>"),
    ],
)
def test_distance_compiles_to_the_pgvector_operator(metric, operator):
    expression = distance_for(metric)(Note.embedding, VECTOR)
    assert operator in _compile(expression, postgresql.dialect())


@pytest.mark.parametrize(
    "element", [cosine_distance, l2_distance, max_inner_product, l1_distance]
)
def test_distance_refuses_to_compile_on_sqlite(element):
    """The regression guard: pgvector's own comparator would emit ``<=>`` here."""
    with pytest.raises(UnsupportedDialectError, match="sqlite"):
        _compile(element(Note.embedding, VECTOR), sqlite.dialect())


def test_comparator_exposes_the_distance_methods():
    assert "<=>" in _compile(
        Note.embedding.cosine_distance(VECTOR), postgresql.dialect()
    )
    assert "<->" in _compile(Note.embedding.l2_distance(VECTOR), postgresql.dialect())
    assert "<#>" in _compile(
        Note.embedding.max_inner_product(VECTOR), postgresql.dialect()
    )
    assert "<+>" in _compile(Note.embedding.l1_distance(VECTOR), postgresql.dialect())


def test_metric_maps_to_pgvector_names():
    assert DistanceMetric.COSINE.pg_opclass == "vector_cosine_ops"
    assert DistanceMetric.L2.pg_opclass == "vector_l2_ops"
    assert DistanceMetric.DOT.pg_opclass == "vector_ip_ops"
    assert DistanceMetric.L1.pg_opclass == "vector_l1_ops"
    assert DistanceMetric.COSINE.sqlite_option == "COSINE"


# --- composability ---


def test_embedding_only_model_has_no_other_columns():
    assert sorted(c.name for c in Note.__table__.c) == ["embedding", "id"]


def test_mixins_compose_into_the_columns_they_add():
    assert sorted(c.name for c in Chunk.__table__.c) == [
        "attributes",
        "embedding",
        "id",
    ]
    assert sorted(c.name for c in Article.__table__.c) == ["id", "text"]


def test_vector_document_carries_every_column():
    assert sorted(c.name for c in Document.__table__.c) == [
        "attributes",
        "collection_id",
        "created_at",
        "embedding",
        "id",
        "text",
        "updated_at",
    ]


def test_vector_collection_is_concrete():
    assert VectorCollection.__tablename__ == "vector_collection"
    assert VectorCollectionRepository.model is VectorCollection


# --- indexes ---


def _index_ddl(model, name, dialect=postgresql.dialect()) -> str:
    index = next(i for i in model.__table__.indexes if i.name == name)
    return _compile(sa.schema.CreateIndex(index), dialect)


def test_embedding_index_uses_hnsw_with_the_metric_opclass():
    ddl = _index_ddl(Note, "ix_test_vectors_note__embedding")
    assert "USING hnsw" in ddl
    assert "vector_cosine_ops" in ddl
    assert "m = 16" in ddl
    assert "ef_construction = 64" in ddl


def test_embedding_index_follows_the_configured_metric():
    assert "vector_l2_ops" in _index_ddl(Chunk, "ix_test_vectors_chunk__embedding")


def test_attributes_index_uses_gin():
    ddl = _index_ddl(Chunk, "ix_test_vectors_chunk__attributes")
    assert "USING gin" in ddl
    assert "jsonb_path_ops" in ddl


def test_text_index_inlines_the_search_configuration():
    ddl = _index_ddl(Article, "ix_test_vectors_article__text")
    assert "USING gin" in ddl
    assert "to_tsvector('english', text)" in ddl


def test_custom_index_options():
    index = Note.embedding_index("custom_name", m=32, ef_construction=128)
    options = index.dialect_options["postgresql"]
    assert index.name == "custom_name"
    assert options["using"] == "hnsw"
    assert options["with"] == {"m": 32, "ef_construction": 128}


def test_invalid_regconfig_is_rejected():
    class Sneaky(TextMixin):
        __text_regconfig__ = "english'; DROP TABLE users --"

    with pytest.raises(ValueError, match="text search configuration"):
        Sneaky.text_document()


@pytest.mark.anyio
async def test_postgresql_only_indexes_are_skipped_on_sqlite(db: Database):
    """``ddl_if`` keeps HNSW and GIN DDL out of the SQLite schema."""
    async with db.engine.begin() as conn:
        await conn.run_sync(Document.__table__.create, checkfirst=True)
        result = await conn.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        )
        names = {row[0] for row in result}
        await conn.run_sync(Document.__table__.drop, checkfirst=True)
    assert "ix_test_vectors_document__embedding" not in names
    assert "ix_test_vectors_document__text" not in names
    assert "ix_test_vectors_document__collection_id" in names


# --- attributes filtering ---


def test_attributes_contain_builds_a_containment_predicate():
    assert "@>" in _compile(
        Chunk.attributes_contain({"lang": "en"}),
        postgresql.dialect(),
        literal_binds=False,
    )


# --- repository model validation ---


@pytest.mark.parametrize(
    ("repository", "model", "missing"),
    [
        (VectorRepository, Article, "EmbeddingMixin"),
        (HybridVectorRepository, Chunk, "TextMixin"),
        (VectorRepository, Plain, "EmbeddingMixin"),
    ],
)
def test_repository_rejects_a_model_without_the_mixin(repository, model, missing):
    with pytest.raises(TypeError, match=missing):

        class Bad(repository[model]):
            pass


def test_repository_accepts_a_model_carrying_the_mixin():
    assert NoteRepository.model is Note
    assert DocumentRepository.model is Document


def test_hybrid_repository_requires_both_mixins():
    assert issubclass(Document, EmbeddingMixin)
    assert issubclass(Document, TextMixin)
    assert issubclass(TextEmbeddingBase, TextMixin)


# --- query builder capabilities ---

PG = PostgresqlQueryBuilder()
SQLITE = SQLiteQueryBuilder()
MYSQL = MysqlQueryBuilder()


@pytest.mark.parametrize(
    ("builder", "option", "supported"),
    [
        (PG, Option.VECTORS, True),
        (PG, Option.FULL_TEXT, True),
        (SQLITE, Option.VECTORS, True),
        (SQLITE, Option.FULL_TEXT, False),
        (MYSQL, Option.VECTORS, False),
        (MYSQL, Option.FULL_TEXT, False),
        (QueryBuilder(), Option.VECTORS, False),
    ],
)
def test_search_capability_claims(builder, option, supported):
    assert builder.supports(option) is supported


def test_a_builder_without_vectors_refuses_to_build_one():
    builder = QueryBuilder()
    with pytest.raises(UnsupportedDialectError, match="vector search"):
        builder.vector_search(Note, VECTOR, limit=5)
    with pytest.raises(UnsupportedDialectError, match="distance"):
        builder.vector_distance(Note, VECTOR)


def test_a_builder_without_full_text_refuses_to_build_one():
    with pytest.raises(UnsupportedDialectError, match="full text search"):
        SQLITE.text_search(Document, "hello", limit=5)
    with pytest.raises(UnsupportedDialectError, match="reciprocal rank fusion"):
        SQLITE.rrf_search(Document, VECTOR, "hello")


# --- search statements ---


def test_pg_search_orders_by_distance():
    sql = _compile(
        PG.vector_search(Note, VECTOR, limit=5),
        postgresql.dialect(),
        literal_binds=False,
    )
    assert "<=>" in sql
    assert "ORDER BY" in sql
    assert "LIMIT" in sql


def test_pg_search_applies_the_filters_it_is_given():
    sql = _compile(
        PG.vector_search(
            Chunk,
            VECTOR,
            Chunk.attributes_contain({"lang": "en"}),
            Chunk.id.is_(None),
            limit=5,
        ),
        postgresql.dialect(),
        literal_binds=False,
    )
    assert "@>" in sql
    assert "IS NULL" in sql


def test_pg_search_honours_a_metric_override():
    sql = _compile(
        PG.vector_search(Note, VECTOR, limit=5, metric=DistanceMetric.L2),
        postgresql.dialect(),
        literal_binds=False,
    )
    assert "<->" in sql


def test_sqlite_search_joins_the_streaming_scan():
    sql = _compile(
        SQLITE.vector_search(Note, VECTOR, limit=5),
        sqlite.dialect(),
        literal_binds=False,
    )
    assert "vector_full_scan" in sql
    assert "rowid" in sql
    assert "ORDER BY" in sql


def test_sqlite_search_keeps_the_filters():
    sql = _compile(
        SQLITE.vector_search(Chunk, VECTOR, Chunk.id.is_(None), limit=5),
        sqlite.dialect(),
        literal_binds=False,
    )
    assert "vector_full_scan" in sql
    assert "IS NULL" in sql


def test_sqlite_rejects_a_metric_the_column_was_not_built_for():
    with pytest.raises(UnsupportedDialectError, match="per column"):
        SQLITE.vector_search(Note, VECTOR, limit=5, metric=DistanceMetric.L2)


def test_sqlite_accepts_the_metric_the_column_was_built_for():
    query = SQLITE.vector_search(Note, VECTOR, limit=5, metric=DistanceMetric.COSINE)
    assert "vector_full_scan" in _compile(query, sqlite.dialect(), literal_binds=False)


def test_sqlite_declares_the_column_per_connection():
    sql = _compile(SQLITE.vector_init(Chunk), sqlite.dialect())
    assert "vector_init" in sql
    assert "dimension=4" in sql
    assert "distance=L2" in sql
    assert "type=FLOAT32" in sql


@pytest.mark.parametrize("builder", [PG, MYSQL, QueryBuilder()])
def test_only_sqlite_needs_a_declaration(builder):
    assert builder.vector_init(Note) is None


def test_pg_text_search_ranks_by_ts_rank():
    sql = _compile(
        PG.text_search(Document, "hello world", limit=5),
        postgresql.dialect(),
        literal_binds=False,
    )
    assert "ts_rank" in sql
    assert "websearch_to_tsquery" in sql
    assert "@@" in sql


def test_rrf_query_fuses_both_rankings():
    sql = _compile(
        PG.rrf_search(Document, VECTOR, "hello", k=60, limit=5, candidates=50),
        postgresql.dialect(),
        literal_binds=False,
    )
    assert "vector_candidates" in sql
    assert "text_candidates" in sql
    assert "FULL OUTER JOIN" in sql
    assert "ORDER BY rrf.score DESC" in sql


def test_identity_column_requires_a_single_primary_key():
    with pytest.raises(TypeError, match="single-column primary key"):
        PG.identity_column(Composite)


# --- dialect guards ---


@pytest.mark.anyio
async def test_search_rejects_an_unsupported_dialect():
    repository = NoteRepository().using(
        db=Database("mysql+asyncmy://user@localhost/db")
    )
    with pytest.raises(UnsupportedDialectError, match="mysql"):
        await repository.search(VECTOR)


@pytest.mark.anyio
async def test_text_search_is_postgresql_only():
    with pytest.raises(UnsupportedDialectError, match="text_search"):
        await DocumentRepository().text_search("hello")


@pytest.mark.anyio
async def test_rrf_search_is_postgresql_only():
    with pytest.raises(UnsupportedDialectError, match="rrf_search"):
        await DocumentRepository().rrf_search(VECTOR, "hello")


@pytest.mark.anyio
async def test_search_rejects_a_metric_override_on_sqlite():
    with pytest.raises(UnsupportedDialectError, match="per column"):
        await NoteRepository().search(VECTOR, metric=DistanceMetric.L2)


# --- loader ---


@pytest.mark.anyio
async def test_init_vectors_rejects_an_unsupported_dialect():
    database = Database("mysql+asyncmy://user@localhost/db")
    with pytest.raises(UnsupportedDialectError, match="mysql"):
        await init_vectors(database)


@pytest.mark.anyio
async def test_register_sqlite_vector_reports_the_missing_package(monkeypatch, db):
    monkeypatch.setitem(sys.modules, "sqlite_vector", None)
    with pytest.raises(ImportError, match="vectors-sqlite"):
        register_sqlite_vector(db.engine)


@pytest.mark.anyio
async def test_init_vectors_loads_the_sqlite_extension():
    """Proof the extension reaches the connection, not just the pool."""
    pytest.importorskip("sqlite_vector")
    database = Database(MEMORY_URL)
    try:
        await init_vectors(database)
        version = await database.execute(sa.select(sa.func.vector_version()))
        assert version.scalar()
    finally:
        await database.dispose()


@pytest.mark.anyio
async def test_registering_the_same_engine_twice_is_a_no_op():
    pytest.importorskip("sqlite_vector")
    database = Database(MEMORY_URL)
    try:
        register_sqlite_vector(database.engine)
        register_sqlite_vector(database.engine)
        version = await database.execute(sa.select(sa.func.vector_version()))
        assert version.scalar()
    finally:
        await database.dispose()
