from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Literal, overload

import sqlalchemy as sa

if TYPE_CHECKING:
    from sqlalchemy.sql._typing import (
        _ColumnExpressionArgument,
        _DMLTableArgument,
        _FromClauseArgument,
    )
    from sqlalchemy.sql.dml import (
        ReturningDelete,
        ReturningInsert,
        ReturningUpdate,
    )

    from .typing import OnConflict, Values, WithForUpdate

from enum import Flag, auto


class Option(Flag):
    NONE = 0
    RETURNING = auto()
    CONFLICTS = auto()
    LOCKS = auto()


class QueryBuilderError(Exception):
    pass


class UnsupportedOption(QueryBuilderError):
    pass


class QueryBuilder:
    supported_options: Option = Option.NONE

    @classmethod
    def supports(cls, option: Option) -> bool:
        return option in cls.supported_options

    def select(
        self,
        *args: Any,
        with_for_update: bool | WithForUpdate | None = None,
        options: tuple[Any, ...] | None = None,
    ) -> sa.Select:
        query = sa.select(*args)
        if with_for_update is True:
            query = query.with_for_update()
        elif with_for_update:
            query = query.with_for_update(**with_for_update)
        if options:
            query = query.options(*options)
        return query

    def _insert(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        on_conflict: OnConflict | None = None,
    ) -> sa.Insert:
        if on_conflict and not self.supports(Option.RETURNING):
            raise UnsupportedOption
        query = sa.insert(table)
        if values is not None:
            query = query.values(values)
        return query

    @overload
    def insert(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        *,
        return_results: Literal[False] = False,
        on_conflict: OnConflict | None = None,
    ) -> sa.Insert: ...

    @overload
    def insert(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        *,
        return_results: Literal[True],
        on_conflict: OnConflict | None = None,
    ) -> ReturningInsert: ...

    @overload
    def insert(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        *,
        return_results: bool = False,
        on_conflict: OnConflict | None = None,
    ) -> sa.Insert | ReturningInsert: ...

    def insert(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        *,
        return_results: bool = False,
        on_conflict: OnConflict | None = None,
    ) -> sa.Insert | ReturningInsert:
        if return_results and not self.supports(Option.RETURNING):
            raise UnsupportedOption
        query = self._insert(table, values, on_conflict)
        if return_results:
            return query.returning(table)
        return query

    @overload
    def update(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        *,
        return_results: Literal[False] = False,
    ) -> sa.Update: ...

    @overload
    def update(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        *,
        return_results: Literal[True],
    ) -> ReturningUpdate: ...

    @overload
    def update(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        *,
        return_results: bool = False,
    ) -> sa.Update | ReturningUpdate: ...

    def update(
        self,
        table: _DMLTableArgument,
        values: Values | None = None,
        *,
        return_results: bool = False,
    ) -> sa.Update | ReturningUpdate:
        if return_results and not self.supports(Option.RETURNING):
            raise UnsupportedOption
        query = sa.update(table)
        if values is not None:
            query = query.values(values)
        if return_results:
            return query.returning(table)
        return query

    @overload
    def delete(
        self, table: _DMLTableArgument, *, return_results: Literal[False] = False
    ) -> sa.Delete: ...

    @overload
    def delete(
        self, table: _DMLTableArgument, *, return_results: Literal[True]
    ) -> ReturningDelete: ...

    @overload
    def delete(
        self, table: _DMLTableArgument, *, return_results: bool = False
    ) -> sa.Delete | ReturningDelete: ...

    def delete(
        self, table: _DMLTableArgument, *, return_results: bool = False
    ) -> sa.Delete | ReturningDelete:
        if return_results and not self.supports(Option.RETURNING):
            raise UnsupportedOption
        query = sa.delete(table)
        if return_results:
            return query.returning(table)
        return query

    def filter(
        self, query: Any, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Any:
        if args:
            query = query.filter(*args)
        if kwargs:
            query = query.filter_by(**kwargs)
        return query

    def count(
        self,
        table: _FromClauseArgument,
        *args: _ColumnExpressionArgument[bool],
        **kwargs: Any,
    ) -> sa.Select[tuple[int, ...]]:
        query = self.select(sa.func.count()).select_from(table)
        return self.filter(query, *args, **kwargs)

    @overload
    def page(
        self,
        query: sa.Select,
        *,
        offset: int = 1,
        limit: int = 100,
        include_total: Literal[True],
    ) -> tuple[sa.Select, sa.Select[tuple[int, ...]]]: ...

    @overload
    def page(
        self,
        query: sa.Select,
        *,
        offset: int = 1,
        limit: int = 100,
        include_total: Literal[False],
    ) -> tuple[sa.Select, None]: ...

    @overload
    def page(
        self,
        query: sa.Select,
        *,
        offset: int = 1,
        limit: int = 100,
        include_total: bool = False,
    ) -> tuple[sa.Select, None] | tuple[sa.Select, sa.Select[tuple[int, ...]]]: ...

    def page(
        self,
        query: sa.Select,
        *,
        offset: int = 1,
        limit: int = 100,
        include_total: bool = False,
    ) -> tuple[sa.Select, sa.Select[tuple[int, ...]] | None]:
        page_query = query.offset(offset).limit(limit)
        total_query = self.count(query.subquery()) if include_total else None
        return page_query, total_query

    def lock(self, key: str) -> sa.TextClause:
        msg = f"Cannot obtain lock for key {key}"
        raise UnsupportedOption(msg)

    def unlock(self, key: str) -> sa.TextClause:
        msg = f"Cannot release lock for key {key}"
        raise UnsupportedOption(msg)

    def get_lock_pair(self, key: str) -> tuple[sa.TextClause, sa.TextClause]:
        if not self.supports(Option.LOCKS):
            raise UnsupportedOption
        return self.lock(key), self.unlock(key)


@functools.cache
def get_query_builder(dialect: str) -> QueryBuilder:
    if dialect == "postgresql":
        from .dialects.postgres import PostgresqlQueryBuilder

        return PostgresqlQueryBuilder()
    if dialect == "sqlite":
        from .dialects.sqlite import SQLiteQueryBuilder

        return SQLiteQueryBuilder()
    if dialect == "mysql":
        from .dialects.mysql import MysqlQueryBuilder

        return MysqlQueryBuilder()
    return QueryBuilder()
