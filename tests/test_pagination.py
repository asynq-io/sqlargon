from typing import ClassVar

from sqlargon.pagination import (
    CursorPaginationStrategy,
    NumberedPaginationStrategy,
    OffsetLimitPaginationStrategy,
    TotalNumberedPaginationStrategy,
    TotalOffsetLimitPaginationStrategy,
)
from sqlargon.pagination.abc import PaginationOptions


def make_paginated_repo(user_repository_class, strategy_cls):
    class Repo(user_repository_class, strategy_cls):
        pass

    return Repo()


async def test_numbered_pagination_empty(user_repository_class):
    repo = make_paginated_repo(user_repository_class, NumberedPaginationStrategy)
    page = await repo.paginate(1, 100)
    assert len(page.items) == 0
    assert page.current_page == 1
    assert page.page_size == 100
    assert page.items_on_page == 0


async def test_numbered_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, NumberedPaginationStrategy)
    await repo.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()
    page = await repo.paginate(1, 2)
    assert len(page.items) == 2
    assert page.current_page == 1
    assert page.page_size == 2
    assert page.items_on_page == 2

    page2 = await repo.paginate(2, 2)
    assert len(page2.items) == 1
    assert page2.current_page == 2
    assert page2.items_on_page == 1


async def test_total_numbered_pagination_empty(user_repository_class):
    repo = make_paginated_repo(user_repository_class, TotalNumberedPaginationStrategy)
    page = await repo.paginate(1, 100)
    assert len(page.items) == 0
    assert page.total_items == 0
    assert page.total_pages == 0


async def test_total_numbered_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, TotalNumberedPaginationStrategy)
    await repo.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()

    page = await repo.paginate(1, 2)
    assert len(page.items) == 2
    assert page.current_page == 1
    assert page.page_size == 2
    assert page.items_on_page == 2
    assert page.total_pages == 2
    assert page.total_items == 3

    page2 = await repo.paginate(2, 2)
    assert len(page2.items) == 1
    assert page2.current_page == 2
    assert page2.items_on_page == 1
    assert page2.total_pages == 2
    assert page2.total_items == 3


async def test_total_numbered_pagination_model_items(user_repository_class):
    repo = make_paginated_repo(user_repository_class, TotalNumberedPaginationStrategy)
    await repo.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()
    page = await repo.paginate(1, 2)
    assert all(isinstance(item, user_repository_class.model) for item in page.items)


async def test_offset_limit_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, OffsetLimitPaginationStrategy)
    await repo.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()
    page = await repo.paginate(0, 2)
    assert len(page.items) == 2

    page2 = await repo.paginate(2, 2)
    assert len(page2.items) == 1


async def test_total_offset_limit_pagination(user_repository_class):
    repo = make_paginated_repo(
        user_repository_class, TotalOffsetLimitPaginationStrategy
    )
    await repo.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()
    page = await repo.paginate(0, 2)
    assert len(page.items) == 2
    assert page.total_items == 3


async def test_cursor_pagination_empty(user_repository_class):
    repo = make_paginated_repo(user_repository_class, CursorPaginationStrategy)
    page = await repo.paginate(None, 100)
    assert len(page.items) == 0
    assert page.next_page is None


async def test_cursor_pagination(user_repository_class):
    repo = make_paginated_repo(user_repository_class, CursorPaginationStrategy)
    await repo.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()
    page = await repo.paginate(None, 2)
    assert len(page.items) == 2
    assert page.next_page is not None
    assert all(isinstance(item, user_repository_class.model) for item in page.items)

    page2 = await repo.paginate(page.next_page, 2)
    assert len(page2.items) == 1
    assert page2.next_page is None


async def test_cursor_pagination_as_mapping(user_repository_class, user_model):
    class Repo(user_repository_class, CursorPaginationStrategy):
        pagination_options: ClassVar[PaginationOptions] = {
            "as_model": False,
            "unique": False,
        }

    repo = Repo()
    await repo.insert(
        [{"name": "John"}, {"name": "Vincent"}, {"name": "Andrew"}]
    ).execute()
    page = await repo.select(user_model.id, user_model.name).paginate(None, 2)
    assert len(page.items) == 2
    assert all(hasattr(item, "__getitem__") for item in page.items)
