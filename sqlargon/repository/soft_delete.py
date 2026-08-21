from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlargon.mixins import SoftDeleteMixin
from sqlargon.orm import SoftDeleteModel

from .base import SQLAlchemyRepository

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.sql._typing import _ColumnExpressionArgument
    from typing_extensions import Self

    from sqlargon.typing import MultipleValues, SingleValue, Values, WithForUpdate

__all__ = ["DeletedRowExistsError", "SoftDeleteRepository"]


class DeletedRowExistsError(RuntimeError):
    """A tombstoned row holds the unique key a new row was to be created with."""


class SoftDeleteRepository(SQLAlchemyRepository[SoftDeleteModel], abstract=True):
    """Repository that tombstones rows instead of deleting them.

    Every statement is scoped to live rows: selects and :meth:`count` skip
    tombstoned rows, :meth:`update` refuses to touch them, and :meth:`delete`
    is rewritten into an update raising the flag -- so ``remove``,
    ``delete_one`` and ``delete_many`` all soft delete::

        class User(UUIDModelMixin, SoftDeleteBase): ...


        class UserRepository(SoftDeleteRepository[User]): ...


        users = UserRepository()

        await users.remove(User.id == user_id)  # UPDATE ... SET tombstone = true
        await users.list()  # the row is gone from reads
        await users.restore(User.id == user_id)  # and back again

    The flag belongs to :meth:`delete` and :meth:`restore` alone: it is left
    out of the default ``ON CONFLICT DO UPDATE`` set, so an upsert cannot
    silently resurrect a deleted row. Reach past the scope with
    :meth:`with_deleted`, :meth:`only_deleted` and :meth:`hard_delete`.

    The model type variable is bound to
    :class:`~sqlargon.mixins.SoftDeleteBase`, so a type checker rejects a
    model that cannot be soft deleted. At runtime the looser
    :class:`~sqlargon.mixins.SoftDeleteMixin` is enough; anything else raises
    ``TypeError`` on subclassing.
    """

    __slots__ = ("deleted_only", "include_deleted")

    def __init__(self) -> None:
        super().__init__()
        self.include_deleted = False
        self.deleted_only = False

    def __init_subclass__(cls, *, abstract: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(abstract=abstract, **kwargs)
        if not abstract and not issubclass(cls.model, SoftDeleteMixin):
            msg = (
                f"{cls.model.__name__} must inherit from SoftDeleteMixin "
                f"to be used with {cls.__name__}"
            )
            raise TypeError(msg)

    @classmethod
    def _get_default_set(cls) -> set[str]:
        return super()._get_default_set() - {"tombstone"}

    @property
    def _not_deleted(self) -> _ColumnExpressionArgument[bool]:
        return self.model.not_deleted

    @property
    def _is_deleted(self) -> _ColumnExpressionArgument[bool]:
        return self.model.is_deleted

    @property
    def _scope(self) -> _ColumnExpressionArgument[bool] | None:
        """The predicate every statement is narrowed to, if any."""
        if self.deleted_only:
            return self._is_deleted
        if self.include_deleted:
            return None
        return self._not_deleted

    def _scoped(self, query: Any) -> Any:
        """Narrow ``query`` to the rows this repository is scoped to."""
        scope = self._scope
        if scope is None:
            return query
        return query.where(scope)

    def copy(self, query: Any) -> Self:
        clone = super().copy(query)
        clone.include_deleted = self.include_deleted
        clone.deleted_only = self.deleted_only
        return clone

    @property
    def query(self) -> Any:
        if self._query is None:
            self.use_query(self._scoped(super().query))
        return self._query

    def select(
        self,
        *args: Any,
        with_for_update: bool | WithForUpdate | None = None,
        options: tuple[Any, ...] | None = None,
    ) -> Self:
        clone = super().select(*args, with_for_update=with_for_update, options=options)
        return clone.use_query(self._scoped(clone.query))

    def update(self, values: Values, *, return_results: bool = False) -> Self:
        clone = super().update(values, return_results=return_results)
        return clone.use_query(self._scoped(clone.query))

    def delete(self, *, return_results: bool = False) -> Self:
        """Raise the tombstone on the matched rows instead of removing them."""
        return self.update({"tombstone": True}, return_results=return_results)

    async def count(self, *args: _ColumnExpressionArgument[bool], **kwargs: Any) -> int:
        scope = self._scope
        if scope is not None:
            args = (*args, scope)
        return await super().count(*args, **kwargs)

    async def bulk_update(
        self,
        values: MultipleValues,
        *args: Any,
        on_: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        scope = self._scope
        if scope is not None:
            args = (*args, scope)
        await super().bulk_update(values, *args, on_=on_, **kwargs)

    async def get_or_create(
        self, defaults: SingleValue | None = None, **kwargs: Any
    ) -> SoftDeleteModel:
        """Get the live row matching ``kwargs`` or create it.

        Raises :class:`DeletedRowExistsError` when the row exists but is
        tombstoned: creating it would violate the unique key, and returning it
        would resurrect a deleted row behind the caller's back.
        """
        async with self.session():
            values = {**(defaults or {}), **kwargs}
            result = await self._insert_returning([values], do="ignore")
            created = result.one_or_none()
            if created is not None:
                return created
            obj = await self.select().filter(**kwargs).one_or_none()
            if obj is None:
                msg = (
                    f"A deleted {self.model.__name__} row already matches "
                    f"{kwargs}; restore or hard delete it first"
                )
                raise DeletedRowExistsError(msg)
            return obj

    def with_deleted(self) -> Self:
        """Return a copy whose statements cover tombstoned rows as well."""
        clone = self.copy(self._query)
        clone.include_deleted = True
        clone.deleted_only = False
        return clone

    def only_deleted(self) -> Self:
        """Return a copy scoped to tombstoned rows.

        The scope holds for every statement the copy builds, so
        ``only_deleted().count()`` counts the trash and
        ``only_deleted().hard_delete()`` empties it.
        """
        clone = self.copy(self._query)
        clone.include_deleted = True
        clone.deleted_only = True
        return clone

    async def restore(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> Sequence[SoftDeleteModel]:
        """Clear the tombstone on the matched rows and return them."""
        return await self.only_deleted().update_many(
            {"tombstone": False}, *args, **kwargs
        )

    async def hard_delete(
        self, *args: _ColumnExpressionArgument[bool], **kwargs: Any
    ) -> None:
        """Physically delete the matched rows, bypassing the tombstone."""
        if self.deleted_only:
            args = (*args, self._is_deleted)
        await super().delete().filter(*args, **kwargs).execute()
