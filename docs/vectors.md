# Vector Search

`sqlargon.vectors` stores embeddings and searches them by similarity. The
column type, the model mixins and the repositories are all separate, so a model
takes only the pieces it needs — an embedding alone, or an embedding beside
text, JSON attributes and a collection.

It requires the `sqlargon[vectors]` extra (`pgvector`), plus
`sqlargon[vectors-sqlite]` (`sqliteai-vector`) to search on SQLite.

```python
from sqlalchemy.orm import declared_attr

from sqlargon import Database
from sqlargon.mixins import UUIDV7ModelMixin
from sqlargon.vectors import EmbeddingBase, VectorRepository, init_vectors


class Note(UUIDV7ModelMixin, EmbeddingBase):
    __vector_dim__ = 384

    @declared_attr.directive
    def __table_args__(cls):
        return (cls.embedding_index(),)


class NoteRepository(VectorRepository[Note]):
    pass


db = Database.from_env()
await init_vectors(db)  # before create_all()
await db.create_all()

await NoteRepository().create(embedding=[0.1] * 384)
nearest = await NoteRepository().search([0.1] * 384, limit=5)
```

## Choosing columns

Each mixin adds one column, its index helper and the expressions that column
needs. Combine only the ones the model wants.

| Mixin | Column | Configure with | Index helper |
| --- | --- | --- | --- |
| `EmbeddingMixin` | `embedding` | `__vector_dim__`, `__vector_distance__` | `embedding_index()` |
| `TextMixin` | `text` | `__text_regconfig__` | `text_index()` |
| `AttributesMixin` | `attributes` | — | `attributes_index()` |
| `VectorCollectionMixin` | `collection_id` | `__collection_table__` | — |

Four abstract bases pre-compose them: `EmbeddingBase` (embedding only),
`TextBase` (text only), `TextEmbeddingBase` (both) and `VectorDocument`
(everything, plus `UUIDV7ModelMixin` and `CreatedUpdatedMixin`). Anything else
is composed directly:

```python
class Chunk(UUIDV7ModelMixin, AttributesMixin, EmbeddingBase):
    """An embedding and JSON attributes -- no text, no collection."""

    __vector_dim__ = 1536
```

Index DDL only runs on PostgreSQL, so the same model is portable: SQLite needs
no index, and on MySQL the embedding degrades to JSON storage with no search.

`VectorDocument` points `collection_id` at `VectorCollection`, a concrete model
this package declares — importing it registers the `vector_collection` table
with the shared metadata, so `create_all()` creates it. Point
`__collection_table__` at a table of your own to group documents differently.

## Choosing a repository

Each repository requires the mixins its search needs and raises `TypeError` at
subclass time when the model lacks one.

| Repository | Model needs | Adds |
| --- | --- | --- |
| `VectorRepository` | `EmbeddingMixin` | `search()` |
| `TextSearchRepository` | `TextMixin` | `text_search()` |
| `HybridVectorRepository` | both | both, plus `rrf_search()` |

## Similarity and hybrid search

`search()` returns the models nearest to a vector, most similar first. Extra
positional expressions and keyword equalities narrow it exactly as `where()`
does, which is all hybrid search is — an ordinary `WHERE` beside the ordering:

```python
found = await documents.search(
    embedding,
    Document.attributes_contain({"lang": "en"}),
    Document.text.like("%apple%"),
    collection_id=collection.id,
    limit=10,
)
```

The filter is applied before the limit, so filtering never returns fewer rows
than it should. `with_distance=True` returns `(model, distance)` pairs:

```python
for document, distance in await documents.search(embedding, with_distance=True):
    print(document.text, distance)
```

Attributes need no repository support — `attributes_contain()` is a predicate,
and the `JSON` column type also offers `contains()`, `has_any_key()` and
`json_value()` through its comparator.

## Distance metrics

`__vector_distance__` sets the metric a model's index is built for and its
searches default to. Distances always sort ascending, so the nearest row comes
first whichever metric is chosen.

| Metric | pgvector operator | Index opclass |
| --- | --- | --- |
| `DistanceMetric.COSINE` (default) | `<=>` | `vector_cosine_ops` |
| `DistanceMetric.L2` | `<->` | `vector_l2_ops` |
| `DistanceMetric.DOT` | `<#>` | `vector_ip_ops` |
| `DistanceMetric.L1` | `<+>` | `vector_l1_ops` |

`DOT` is the *negative* inner product, which is what keeps ascending order
meaningful for it.

On PostgreSQL a single query can override the metric with `search(...,
metric=DistanceMetric.L2)`, though it will not use an index built for another
one. SQLite fixes the metric per column, so overriding it there raises
`UnsupportedDialectError`.

Outside a repository the comparator gives the same expressions directly, for
instance `Note.embedding.cosine_distance(vector)`. They compile on PostgreSQL
only; on other dialects they raise `UnsupportedDialectError` rather than
emitting SQL the server would reject.

## Full text and reciprocal rank fusion

`TextSearchRepository.text_search()` ranks rows by `ts_rank` over
`to_tsvector(__text_regconfig__, text)` — the same expression `text_index()`
builds, so the index applies. `HybridVectorRepository.rrf_search()` fuses that
ranking with the vector one by reciprocal rank fusion: it ranks the `candidates`
nearest rows and the `candidates` best text matches, then scores each row
`sum(1 / (k + rank))` over the rankings it appears in, so a row both agree on
outranks one that only either found.

```python
for document, score in await documents.rrf_search(embedding, "red apple", limit=10):
    print(score, document.text)
```

Both are PostgreSQL only and raise `UnsupportedDialectError` elsewhere.

## Setting a backend up

`init_vectors(db)` prepares a database and has to run before `create_all()` —
a `VECTOR` column cannot be declared before the type exists. On PostgreSQL it
issues `CREATE EXTENSION IF NOT EXISTS vector`; on SQLite it registers the
loadable extension on the engine's pool, so register it at startup, before any
query, because connections checked out earlier never get it.

Applications that manage their schema with alembic create the extension in a
migration instead, ahead of the table:

```python
def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
```

Index DDL belongs in the migration too, since `create_all()` is not what builds
the schema there.

## Backend support

| | PostgreSQL | SQLite | MySQL / MariaDB |
| --- | --- | --- | --- |
| Storage | `VECTOR(n)` | float32 `BLOB` | JSON |
| `search()` | distance operator | `vector_full_scan` join | not supported |
| `text_search()` / `rrf_search()` | yes | no | no |
| Indexes | HNSW, GIN | none needed | none |

SQLite goes through sqlite-vector, which differs enough to be worth knowing:
it has no scalar distance function, so searches join its table valued scan
rather than ordering by an expression; the metric is fixed per column when the
column is declared to it; and vectors are plain float32 blobs. The declaration
happens on first search per connection and is remembered for that connection.

Values read back are `list[float]` on every backend.

Two things are deliberately left out for now: sqlite-vector's quantized scan
(`vector_quantize_scan`), which needs a quantization lifecycle of its own, and
fusing SQLite FTS5 with vector ranking.

## Where the statements come from

The repositories build no SQL of their own. Each statement comes from the
dialect's [`QueryBuilder`](reference/dialects.md), which is what makes the two
backends' very different shapes interchangeable behind one `search()`:

| Hook | Answers |
| --- | --- |
| `vector_search()` | the whole similarity query, filters included |
| `vector_distance()` | the ordering expression, where the backend has one |
| `vector_init()` | the per-connection declaration, or `None` when unneeded |
| `text_search()` | the ranked full text query |
| `rrf_search()` | the fused query |

A repository asks `supports(Option.VECTORS)` or `Option.FULL_TEXT` before
building anything, so an unsupported backend raises
`UnsupportedDialectError` naming the dialect instead of emitting SQL the
server would reject. Teaching sqlargon another vector backend therefore means
overriding these hooks on that dialect's builder and claiming the options —
no repository change at all.
