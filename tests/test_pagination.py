import pytest
import sqlalchemy as sa
from sqlalchemy.orm import aliased

from sqlargon import Database
from sqlargon.pagination import (
    LimitOffsetPagination,
    PageNumberPagination,
    TotalLimitOffsetPagination,
    TotalPageNumberPagination,
)
from sqlargon.pagination.cursor import CursorPagination

USER_DATA = [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]


def make_paginated_repo(user_repository_class, strategy):
    class Repo(user_repository_class):
        paginate = strategy

    return Repo()


async def test_page_number_pagination_empty(user_repository_class):
    repo = make_paginated_repo(user_repository_class, PageNumberPagination())
    page = await repo.paginate(1, 100)
    assert page.items_on_page == 0
    assert page.current_page == 1
    assert page.page_size == 100
    assert page.has_more is False


async def test_page_number_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, PageNumberPagination())
    await repo.insert(USER_DATA).execute()

    page = await repo.paginate(page=1, page_size=2)
    assert page.items_on_page == 2
    assert page.current_page == 1
    assert page.page_size == 2
    assert page.has_more is True
    assert all(isinstance(item, user_repository_class.model) for item in page.items)

    page2 = await repo.paginate(page=2, page_size=2)
    assert page2.items_on_page == 1
    assert page2.has_more is False


async def test_total_page_number_pagination_empty(user_repository_class):
    repo = make_paginated_repo(user_repository_class, TotalPageNumberPagination())
    page = await repo.paginate(1, 100)
    assert page.items_on_page == 0
    assert page.total_items == 0
    assert page.total_pages == 0
    assert page.has_more is False


@pytest.mark.parametrize(
    "strategy_class", [TotalPageNumberPagination, TotalLimitOffsetPagination]
)
async def test_total_pagination_skips_page_query_when_count_is_zero(
    user_repository_class, db: Database, strategy_class
):
    repo = make_paginated_repo(user_repository_class, strategy_class())
    statements: list[str] = []

    def before_execute(_conn, clauseelement, _multiparams, _params, _options):
        statements.append(str(clauseelement))

    sa.event.listen(db.engine.sync_engine, "before_execute", before_execute)
    try:
        page = await repo.paginate()
    finally:
        sa.event.remove(db.engine.sync_engine, "before_execute", before_execute)

    assert page.total_items == 0
    assert list(page.items) == []
    # the page query is skipped entirely when the count says there is nothing
    assert len(statements) == 1
    assert "count" in statements[0].lower()


async def test_total_page_number_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, TotalPageNumberPagination())
    await repo.insert(USER_DATA).execute()

    page = await repo.paginate(page=1, page_size=2)
    assert page.items_on_page == 2
    assert page.total_items == 3
    assert page.total_pages == 2
    assert page.has_more is True

    page2 = await repo.paginate(page=2, page_size=2)
    assert page2.items_on_page == 1
    assert page2.total_items == 3
    assert page2.total_pages == 2
    assert page2.has_more is False


async def test_limit_offset_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, LimitOffsetPagination())
    await repo.insert(USER_DATA).execute()

    page = await repo.paginate(offset=0, limit=2)
    assert page.items_on_page == 2
    assert page.offset == 0
    assert page.limit == 2
    assert page.has_more is True

    page2 = await repo.paginate(offset=2, limit=2)
    assert page2.items_on_page == 1
    assert page2.has_more is False


async def test_total_limit_offset_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, TotalLimitOffsetPagination())
    await repo.insert(USER_DATA).execute()

    page = await repo.paginate(offset=0, limit=2)
    assert page.items_on_page == 2
    assert page.total_items == 3
    assert page.has_more is True

    page2 = await repo.paginate(offset=2, limit=2)
    assert page2.items_on_page == 1
    assert page2.total_items == 3
    assert page2.has_more is False


async def test_pagination_as_mapping(user_repository_class, user_model):
    repo = make_paginated_repo(user_repository_class, PageNumberPagination())
    await repo.insert(USER_DATA).execute()
    page = await repo.select(user_model.id, user_model.name).paginate(as_model=False)
    assert page.items_on_page == 3
    assert all(hasattr(item, "__getitem__") for item in page.items)


async def test_page_number_pages_iterator(user_repository_class):
    repo = make_paginated_repo(user_repository_class, PageNumberPagination())
    await repo.insert(USER_DATA).execute()
    pages = [page async for page in repo.paginate.pages(page_size=2)]
    assert [page.items_on_page for page in pages] == [2, 1]
    assert [page.current_page for page in pages] == [1, 2]


async def test_limit_offset_pages_iterator(user_repository_class):
    repo = make_paginated_repo(user_repository_class, LimitOffsetPagination())
    await repo.insert(USER_DATA).execute()
    pages = [page async for page in repo.paginate.pages(limit=2)]
    assert [page.items_on_page for page in pages] == [2, 1]
    assert [page.offset for page in pages] == [0, 2]


async def test_cursor_pagination_empty(user_repository_class):
    repo = make_paginated_repo(user_repository_class, CursorPagination())
    page = await repo.paginate(None, 100)
    assert page.items_on_page == 0
    assert page.next_page is None
    assert page.has_next is False


async def test_cursor_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, CursorPagination())
    await repo.insert(USER_DATA).execute()

    page = await repo.paginate(None, 2)
    assert page.items_on_page == 2
    assert page.cursor is None
    assert page.has_next is True
    assert all(isinstance(item, user_repository_class.model) for item in page.items)

    page2 = await repo.paginate(page.next_page, 2)
    assert page2.items_on_page == 1
    assert page2.has_next is False
    assert page2.has_previous is True


async def test_cursor_pagination_as_mapping(user_repository_class, user_model):
    repo = make_paginated_repo(user_repository_class, CursorPagination())
    await repo.insert(USER_DATA).execute()
    page = await repo.select(user_model.id, user_model.name).paginate(
        None, 2, as_model=False
    )
    assert page.items_on_page == 2
    assert all(hasattr(item, "__getitem__") for item in page.items)


async def test_cursor_pages_iterator(user_repository_class):
    repo = make_paginated_repo(user_repository_class, CursorPagination())
    await repo.insert(USER_DATA).execute()
    pages = [page async for page in repo.paginate.pages(page_size=2)]
    assert [page.items_on_page for page in pages] == [2, 1]


async def test_strategy_bind_standalone(user_repository_class):
    repo = user_repository_class()
    await repo.insert(USER_DATA).execute()
    paginator = PageNumberPagination().bind(repo)
    page = await paginator(page=1, page_size=2)
    assert page.items_on_page == 2


def test_class_level_access_returns_strategy(user_repository_class):
    class Repo(user_repository_class):
        paginate = PageNumberPagination(default_page_size=10)

    assert isinstance(Repo.paginate, PageNumberPagination)
    assert Repo.paginate.default_page_size == 10


async def test_total_page_number_pages_iterator(user_repository_class):
    repo = make_paginated_repo(user_repository_class, TotalPageNumberPagination())
    await repo.insert(USER_DATA).execute()
    pages = [page async for page in repo.paginate.pages(page_size=2)]
    assert [page.items_on_page for page in pages] == [2, 1]
    assert [page.current_page for page in pages] == [1, 2]
    assert [page.total_items for page in pages] == [3, 3]
    assert [page.total_pages for page in pages] == [2, 2]


async def test_total_limit_offset_pages_iterator(user_repository_class):
    repo = make_paginated_repo(user_repository_class, TotalLimitOffsetPagination())
    await repo.insert(USER_DATA).execute()
    pages = [page async for page in repo.paginate.pages(limit=2)]
    assert [page.items_on_page for page in pages] == [2, 1]
    assert [page.offset for page in pages] == [0, 2]
    assert [page.total_items for page in pages] == [3, 3]


@pytest.mark.parametrize(
    "strategy_class", [TotalPageNumberPagination, TotalLimitOffsetPagination]
)
async def test_total_pages_iterator_on_empty_table(
    user_repository_class, strategy_class
):
    repo = make_paginated_repo(user_repository_class, strategy_class())
    pages = [page async for page in repo.paginate.pages(2)]
    assert len(pages) == 1
    assert list(pages[0].items) == []
    assert pages[0].total_items == 0
    assert pages[0].has_more is False


async def test_pagination_unique_deduplicates_joined_rows(
    user_repository_class, user_model
):
    class Repo(user_repository_class):
        paginate = PageNumberPagination()
        paginate_unique = PageNumberPagination(unique=True)

    repo = Repo()
    await repo.insert(
        [{"name": "John", "last_name": "Doe"}, {"name": "Jane", "last_name": "Doe"}]
    ).execute()
    relative = aliased(user_model)
    onclause = relative.last_name == user_model.last_name

    duplicated = await repo.select().join(relative, onclause).paginate(page_size=10)
    assert duplicated.items_on_page == 4

    deduplicated = (
        await repo.select().join(relative, onclause).paginate_unique(page_size=10)
    )
    assert deduplicated.items_on_page == 2
    assert {item.name for item in deduplicated.items} == {"John", "Jane"}
