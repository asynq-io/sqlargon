from sqlargon.pagination import (
    LimitOffsetPagination,
    PageNumberPagination,
    TotalLimitOffsetPagination,
    TotalPageNumberPagination,
)
from sqlargon.pagination.cursor import CursorPagination

from .models import User, UserRepository

NAMES = ("Andrew", "Bob", "John", "Peter", "Vincent")
PAGE_SIZE = 2


class NumberedUserRepository(UserRepository):
    paginate = PageNumberPagination(default_page_size=PAGE_SIZE)


class TotalNumberedUserRepository(UserRepository):
    paginate = TotalPageNumberPagination(default_page_size=PAGE_SIZE)


class OffsetUserRepository(UserRepository):
    paginate = LimitOffsetPagination(default_page_size=PAGE_SIZE)


class TotalOffsetUserRepository(UserRepository):
    paginate = TotalLimitOffsetPagination(default_page_size=PAGE_SIZE)


class CursorUserRepository(UserRepository):
    paginate = CursorPagination(default_page_size=PAGE_SIZE)


async def seed(users: UserRepository, *names: str) -> None:
    await users.bulk_create([{"name": name} for name in names], return_results=False)


async def test_page_number_pagination(users: UserRepository):
    await seed(users, *NAMES)
    repository = NumberedUserRepository()
    page = await repository.paginate(1)
    assert [user.name for user in page.items] == ["Andrew", "Bob"]
    assert page.has_more is True

    last = await NumberedUserRepository().paginate(3)
    assert [user.name for user in last.items] == ["Vincent"]
    assert last.has_more is False


async def test_page_number_pagination_on_an_empty_table():
    page = await NumberedUserRepository().paginate(1)
    assert page.items_on_page == 0
    assert page.has_more is False


async def test_total_page_number_pagination(users: UserRepository):
    await seed(users, *NAMES)
    page = await TotalNumberedUserRepository().paginate(2)
    assert [user.name for user in page.items] == ["John", "Peter"]
    assert page.total_items == len(NAMES)
    assert page.total_pages == 3
    assert page.has_more is True


async def test_limit_offset_pagination(users: UserRepository):
    await seed(users, *NAMES)
    page = await OffsetUserRepository().paginate(offset=1, limit=2)
    assert [user.name for user in page.items] == ["Bob", "John"]
    assert page.has_more is True


async def test_total_limit_offset_pagination(users: UserRepository):
    await seed(users, *NAMES)
    page = await TotalOffsetUserRepository().paginate(offset=4, limit=2)
    assert [user.name for user in page.items] == ["Vincent"]
    assert page.total_items == len(NAMES)
    assert page.has_more is False


async def test_pages_iterator_walks_every_row(users: UserRepository):
    await seed(users, *NAMES)
    walked = [
        user.name
        async for page in NumberedUserRepository().paginate.pages()
        for user in page.items
    ]
    assert walked == list(NAMES)


async def test_pagination_as_mappings(users: UserRepository):
    await seed(users, *NAMES)
    repository = NumberedUserRepository()
    page = await repository.use_query(
        repository.qb.select(User.name).order_by(User.name)
    ).paginate(1, as_model=False)
    assert [row["name"] for row in page.items] == ["Andrew", "Bob"]


async def test_cursor_pagination_walks_forwards(users: UserRepository):
    await seed(users, *NAMES)
    first = await CursorUserRepository().paginate()
    assert [user.name for user in first.items] == ["Andrew", "Bob"]
    assert first.has_next is True

    second = await CursorUserRepository().paginate(first.next_page)
    assert [user.name for user in second.items] == ["John", "Peter"]
    assert second.has_previous is True


async def test_cursor_pages_iterator_walks_every_row(users: UserRepository):
    await seed(users, *NAMES)
    walked = [
        user.name
        async for page in CursorUserRepository().paginate.pages()
        for user in page.items
    ]
    assert walked == list(NAMES)


async def test_cursor_pagination_on_an_empty_table():
    page = await CursorUserRepository().paginate()
    assert page.items_on_page == 0
    assert page.has_next is False
