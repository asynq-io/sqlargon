from __future__ import annotations

from collections.abc import Mapping as MappingABC
from typing import TYPE_CHECKING, Any

from sqlalchemy import Text, cast

from sqlargon.mixins import VersionedMixin
from sqlargon.orm import VersionedModel

from .base import SQLAlchemyRepository

if TYPE_CHECKING:
    from sqlalchemy.sql._typing import _ColumnExpressionArgument
    from typing_extensions import Self

    from sqlargon.typing import MultipleValues, SingleValue, Values

__all__ = ["ConcurrentModificationError", "VersionedRepository"]


class ConcurrentModificationError(RuntimeError):
    """An update or delete matched zero rows — the version was stale."""


class VersionedRepository(SQLAlchemyRepository[VersionedModel], abstract=True):
    """Repository that auto-increments the version column on every update
    and offers optimistic concurrency checks via
    :meth:`update_if_match` / :meth:`delete_if_match`.

    Works with both :class:`~sqlargon.mixins.UUIDVersionedMixin` (UUID
    column, client-managed) and
    :class:`~sqlargon.mixins.XminVersionedMixin` (PostgreSQL ``xmin``,
    server-managed) — the strategy is read from the SQLAlchemy mapper at
    class creation time::

        class User(UUIDModelMixin, VersionedBase): ...


        class UserRepository(VersionedRepository[User]): ...


        users = UserRepository()

        user = await users.get(id=user_id)
        updated = await users.update_if_match(
            {"name": "jane"}, User.id == user_id, expected_version=user.version_id
        )
        if updated is None:
            # someone else modified the row first

    The version column is left out of the default ``ON CONFLICT DO
    UPDATE`` set, so an upsert cannot silently clobber a version. Regular
    ``update_one`` / ``update_many`` / ``bulk_update`` all auto-increment
    the version but do **not** check it — use ``update_if_match`` /
    ``delete_if_match`` for the optimistic guard, or add a manual
    ``Model.version_id == expected`` filter to any method.

    The model type variable is bound to
    :class:`~sqlargon.orm.VersionedBase`, so a type checker rejects a
    model that cannot be versioned. At runtime the looser
    :class:`~sqlargon.mixins.VersionedMixin` is enough; anything else
    raises ``TypeError`` on subclassing.
    """

    def __init_subclass__(cls, *, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(abstract=abstract, **kwargs)
        if abstract:
            return
        mixin, name = cls._required_mixin()
        if not issubclass(cls.model, mixin):
            msg = (
                f"{cls.model.__name__} must inherit from {name} "
                f"to be used with {cls.__name__}"
            )
            raise TypeError(msg)
        if cls._version_col() is None:
            msg = (
                f"{cls.model.__name__} maps no version column: its "
                f"__mapper_args__ carry no version_id_col, so "
                f"{cls.__name__} could not tell a stale row from a fresh one"
            )
            raise TypeError(msg)

    @classmethod
    def _required_mixin(cls) -> tuple[type, str]:
        """The mixin a model must carry, and how to name it when it does not."""
        return (
            VersionedMixin,
            "VersionedMixin (UUIDVersionedMixin or XminVersionedMixin)",
        )

    @classmethod
    def _version_col(cls) -> Any:
        """The version column from the mapper, or ``None``."""
        return getattr(cls.model.__mapper__, "version_id_col", None)

    @classmethod
    def _version_generator(cls) -> Any:
        """The version generator: ``False`` (server-side), ``True``
        (integer increment), or a callable."""
        return getattr(cls.model.__mapper__, "version_id_generator", True)

    @classmethod
    def _is_server_versioned(cls) -> bool:
        return cls._version_generator() is False

    @classmethod
    def _get_default_set(cls) -> set[str]:
        col = cls._version_col()
        excluded = {col.name} if col is not None else set()
        return super()._get_default_set() - excluded

    def _with_version_increment(self, values: Values) -> Values:
        """Return ``values`` with the version column bumped."""
        col = self._version_col()
        generator = self._version_generator()
        if col is None or generator is False:
            return values
        name = col.name
        if callable(generator):
            if isinstance(values, MappingABC):
                return {**values, name: generator(None)}
            return [{**row, name: generator(None)} for row in values]
        # generator is True — integer increment via SQL expression
        if isinstance(values, MappingABC):
            return {**values, name: col + 1}
        # integer increment is not supported for executemany
        return values

    def _version_filter(self, expected: Any) -> Any:
        """The guard matching ``expected`` against the version column.

        A server managed version is PostgreSQL's ``xmin``, an ``xid`` no
        driver binds natively: asyncpg decodes it into an ``int`` while a
        version round tripped through a client comes back as a ``str``, and
        ``xid = varchar`` is not an operator PostgreSQL has. Comparing the
        column as text accepts either.
        """
        col = self._version_col()
        if self._is_server_versioned():
            return cast(col, Text) == str(expected)
        return col == expected

    def update(self, values: Values, *, return_results: bool = False) -> Self:
        values = self._with_version_increment(values)
        return super().update(values, return_results=return_results)

    async def bulk_update(
        self,
        values: MultipleValues,
        *args: Any,
        on_: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        col = self._version_col()
        generator = self._version_generator()
        if col is not None and callable(generator):
            name = col.name
            values = [{**row, name: generator(None)} for row in values]
        await super().bulk_update(values, *args, on_=on_, **kwargs)

    async def update_if_match(
        self,
        values: SingleValue,
        *args: _ColumnExpressionArgument[bool],
        expected_version: Any,
        raise_on_mismatch: bool = False,
        **kwargs: Any,
    ) -> VersionedModel | None:
        """Update with ``WHERE version_col = expected_version``.

        Returns the updated model, or ``None`` if no row matched (the
        version was stale or the row is gone). Raises
        :class:`ConcurrentModificationError` when ``raise_on_mismatch`` is
        ``True`` and no row matched.
        """
        filters = (*args, self._version_filter(expected_version))
        result = await self._update_returning(values, *filters, **kwargs)
        row = result.one_or_none()
        if row is None and raise_on_mismatch:
            msg = (
                f"{self.model.__name__} with version {expected_version!r} "
                "was modified or deleted by a concurrent transaction"
            )
            raise ConcurrentModificationError(msg)
        return row

    async def delete_if_match(
        self,
        *args: _ColumnExpressionArgument[bool],
        expected_version: Any,
        raise_on_mismatch: bool = False,
        **kwargs: Any,
    ) -> VersionedModel | None:
        """Delete with ``WHERE version_col = expected_version``.

        Returns the deleted model, or ``None`` if no row matched. Raises
        :class:`ConcurrentModificationError` when ``raise_on_mismatch`` is
        ``True`` and no row matched.
        """
        filters = (*args, self._version_filter(expected_version))
        result = await self._delete_returning(*filters, **kwargs)
        row = result.one_or_none()
        if row is None and raise_on_mismatch:
            msg = (
                f"{self.model.__name__} with version {expected_version!r} "
                "was modified or deleted by a concurrent transaction"
            )
            raise ConcurrentModificationError(msg)
        return row
