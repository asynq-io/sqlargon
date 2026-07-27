# Pagination

Pagination is a *strategy* attached to a repository class. A strategy is an
immutable configuration object acting as a descriptor: accessed on a
repository instance it returns a paginator bound to it, fully typed with the
repository's model:

```python
from sqlargon import SQLAlchemyRepository
from sqlargon.pagination import PageNumberPagination


class UserRepository(SQLAlchemyRepository[User]):
    paginate = PageNumberPagination(default_page_size=25)


page = await UserRepository().filter(User.is_active).paginate(page=2)
# NumberedPage[User] -- page.items is Sequence[User]
```

Most repositories use exactly one strategy, matching the shape of their API:

| Strategy | Call | Returns |
| --- | --- | --- |
| `PageNumberPagination` | `paginate(page=3, page_size=25)` | `NumberedPage[T]` |
| `TotalPageNumberPagination` | `paginate(page=3, page_size=25)` | `TotalNumberedPage[T]` |
| `LimitOffsetPagination` | `paginate(offset=200, limit=100)` | `OffsetPage[T]` |
| `TotalLimitOffsetPagination` | `paginate(offset=200, limit=100)` | `TotalOffsetPage[T]` |
| `CursorPagination` | `paginate(cursor=token, page_size=100)` | `CursorPage[T]` |

As a rule of thumb: page numbers for classic UIs, offset/limit for background
processing and internal APIs, cursors for large or fast-moving datasets.

## Pages

Pages are frozen dataclasses, generic over the item type. All of them carry
`items` and an `items_on_page` property; the non-cursor pages also carry
`has_more`, detected by over-fetching a single row — no COUNT query is
issued. The `Total*` strategies additionally run a COUNT in the same session
as the page query and report `total_items` (and `total_pages` for numbered
pagination).

Page numbers are 1-based. Paging inputs are **not validated** by the
library; constrain them at the API boundary (see [FastAPI](#fastapi) below).

## Cursor pagination

Cursor (keyset) pagination requires the `sqlakeyset` package (the
`sqlargon[pagination]` extra) and is imported from its own module:

```python
from sqlargon.pagination.cursor import CursorPagination


class LogRepository(SQLAlchemyRepository[LogEntry]):
    default_order_by = LogEntry.id
    paginate = CursorPagination(default_page_size=500)


page = await LogRepository().paginate()  # first page
page = await LogRepository().paginate(page.next_page)
```

Cursors are opaque bookmarks: paging stays stable under concurrent inserts
and cheap at any depth, but requires the query to have a deterministic
`ORDER BY` (e.g. via `default_order_by`). `CursorPage` carries `cursor`,
`next_page` and `previous_page` (`None` at the edges), plus `has_next` /
`has_previous` properties.

## Mappings instead of models

Every paginator accepts `as_model` — with `False` the items are SQLAlchemy
`RowMapping` objects instead of model instances, and the page type follows
the flag:

```python
page = await repo.select(User.id, User.name).paginate(as_model=False)
# NumberedPage[RowMapping]
```

## Iterating over all pages

Paginators expose `pages()`, an async iterator that keeps fetching until the
last page (following `has_more`, or `next_page` for cursors):

```python
async for page in repo.paginate.pages(page_size=1000):
    await process(page.items)
```

## Filters and routing

Pagination composes with the rest of the repository API — it paginates the
repository's current query and follows its routing preference:

```python
await repo.filter(User.is_active).using(read_only=True).paginate(page=1)
```

Pagination never forces `read_only` routing by itself; opt in with
`using(...)` as with any other read.

## Multiple or standalone strategies

A repository can expose several strategies under different names, and a
strategy can be bound to a repository ad hoc without a class attribute:

```python
class UserRepository(SQLAlchemyRepository[User]):
    paginate = TotalPageNumberPagination()
    paginate_cursor = CursorPagination()


paginator = LimitOffsetPagination().bind(UserRepository())
batch = await paginator(offset=500, limit=100)
```

## Configuration

Strategies are immutable and shareable; they take:

- `default_page_size` — used when the call omits `page_size` / `limit`
  (default `100`).
- `unique` — apply `.unique()` to the results, required with joined eager
  loads (default `False`; not applicable to cursor pagination).

## FastAPI

Validate paging inputs with pydantic types in the endpoint signature; the
repository plugs in as a dependency:

```python
from typing import Annotated

from annotated_types import Interval
from fastapi import Depends, FastAPI

PageNumber = Annotated[int, Interval(ge=1)]
PageSize = Annotated[int, Interval(ge=1, le=100)]

app = FastAPI()


@app.get("/users", response_model=UsersPage)
async def list_users(
    repo: Annotated[UserRepository, Depends()],
    page: PageNumber = 1,
    page_size: PageSize = 25,
) -> TotalNumberedPage[UserModel]:
    return await repo.paginate(page, page_size)
```
