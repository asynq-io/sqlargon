import pytest

from sqlargon.pagination import (
    NumberedPaginationStrategy,
    PaginationStrategy,
    TokenPaginationStrategy,
)

pytestmark = pytest.mark.anyio


def get_user_repository(
    user_repository_class, db, paginator_class: type[PaginationStrategy]
):
    class Repository(user_repository_class):
        paginator = paginator_class()

    user_repository = Repository(db)
    return user_repository


async def test_paginate_empty_repository_with_numbered_pagination(
    user_repository_class, db
):
    user_repository = get_user_repository(
        user_repository_class, db, NumberedPaginationStrategy
    )
    page = await user_repository.get_page(page=1)
    assert len(page.items) == 0
    assert page.current_page == 1
    assert page.page_size == 0
    assert page.total_pages == 0
    assert page.total_items == 0


async def test_paginate_repository_with_numbered_pagination(user_repository_class, db):
    user_repository = get_user_repository(
        user_repository_class, db, NumberedPaginationStrategy
    )
    users = [
        {"name": "John"},
        {"name": "Vincent"},
        {"name": "Andrew"},
    ]
    await user_repository.insert(users)
    page = await user_repository.get_page(page=1, page_size=2)
    assert len(page.items) == 2
    assert page.current_page == 1
    assert page.page_size == 2
    assert page.total_pages == 2
    assert page.total_items == 3

    page2 = await user_repository.get_page(page=2, page_size=2)
    assert len(page2.items) == 1
    assert page2.current_page == 2
    assert page2.page_size == 1
    assert page2.total_pages == 2
    assert page2.total_items == 3


async def test_paginate_repository_with_numbered_pagination_without_model(
    user_model, user_repository_class, db
):
    user_repository = get_user_repository(
        user_repository_class, db, NumberedPaginationStrategy
    )
    users = [
        {"name": "John"},
        {"name": "Vincent"},
        {"name": "Andrew"},
    ]
    await user_repository.insert(users)
    page = await user_repository.select(user_model.id, user_model.name).get_page(
        page=1, page_size=2, as_model=False
    )
    assert len(page.items) == 2
    assert page.current_page == 1
    assert page.page_size == 2
    assert page.total_pages == 2
    assert page.total_items == 3

    page2 = await user_repository.select(user_model.id, user_model.name).get_page(
        page=2, page_size=2, as_model=False
    )
    assert len(page2.items) == 1
    assert page2.current_page == 2
    assert page2.page_size == 1
    assert page2.total_pages == 2
    assert page2.total_items == 3


async def test_paginate_empty_repository_with_token_pagination(
    user_repository_class, db
):
    user_repository = get_user_repository(
        user_repository_class, db, TokenPaginationStrategy
    )
    page = await user_repository.get_page()
    assert len(page.items) == 0
    assert page.next_page is None


async def test_paginate_repository_with_token_pagination(user_repository_class, db):
    user_repository = get_user_repository(
        user_repository_class, db, TokenPaginationStrategy
    )

    users = [
        {"name": "John"},
        {"name": "Vincent"},
        {"name": "Andrew"},
    ]
    await user_repository.insert(users)
    page = await user_repository.get_page(page_size=2)
    assert len(page.items) == 2
    assert page.next_page == ">s:John"
    page2 = await user_repository.get_page(page=page.next_page)
    assert len(page2.items) == 1
    assert page2.next_page is None


async def test_paginate_repository_with_token_pagination_without_model(
    user_model, user_repository_class, db
):
    user_repository = get_user_repository(
        user_repository_class, db, TokenPaginationStrategy
    )

    users = [
        {"name": "John"},
        {"name": "Vincent"},
        {"name": "Andrew"},
    ]
    await user_repository.insert(users)
    page = await user_repository.select(user_model.id, user_model.name).get_page(
        page_size=2, as_model=False
    )
    assert len(page.items) == 2
    assert page.next_page == ">s:John"
    page2 = await user_repository.select(user_model.id, user_model.name).get_page(
        page=page.next_page, as_model=False
    )
    assert len(page2.items) == 1
    assert page2.next_page is None
