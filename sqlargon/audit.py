"""Relating other tables to an append-only, versioned model.

A row of an :class:`~sqlargon.orm.AnyAuditableBase` model is one version of
an entity, so a reference to it has to say *which* version it means. The
helpers here cover the two answers:

* :func:`version_mapped_column` and :func:`version_foreign_key` pin a child
  to one exact version, under a real composite foreign key;
* :func:`latest_relationship` follows the entity forward, resolving to
  whichever version is newest at read time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.orm import declared_attr, mapped_column, relationship

if TYPE_CHECKING:
    from sqlalchemy.orm import MappedColumn, Relationship

    from sqlargon.mixins import AuditableMixin

__all__ = ["latest_relationship", "version_foreign_key", "version_mapped_column"]


def _remote_columns(
    target: type[AuditableMixin], columns: tuple[str, ...]
) -> tuple[str, ...]:
    remote = target.audit_key()
    if len(columns) != len(remote):
        msg = (
            f"{target.__name__} is identified by {remote}, so {len(remote)} "
            f"local column(s) are needed, got {len(columns)}: {columns}"
        )
        raise ValueError(msg)
    return remote


def version_mapped_column(
    target: type[AuditableMixin], **kwargs: Any
) -> MappedColumn[Any]:
    """A column holding a version of ``target``, typed to match it.

    The child never has to know which versioning strategy the parent uses::

        class Comment(UUIDModelMixin, Base):
            article_id: Mapped[UUID] = mapped_column(GUID())
            article_version = version_mapped_column(Article)
    """
    return mapped_column(target.__table__.c.version.type, **kwargs)


def version_foreign_key(
    target: type[AuditableMixin], *columns: str, **kwargs: Any
) -> sa.ForeignKeyConstraint:
    """A composite foreign key pinning ``columns`` to one version of ``target``.

    ``columns`` names the local columns mirroring the entity key and the
    version, in that order::

        class Comment(UUIDModelMixin, Base):
            article_id: Mapped[UUID] = mapped_column(GUID())
            article_version = version_mapped_column(Article)

            __table_args__ = (
                version_foreign_key(Article, "article_id", "article_version"),
            )

            article: Mapped[Article] = relationship()

    Because the key is real, the relationship needs no ``primaryjoin`` and is
    writable: assigning an ``Article`` fills both columns. It also stops
    :meth:`~sqlargon.repository.AuditableRepository.purge` from removing a
    version something still points at, unless declared ``ondelete="CASCADE"``.
    """
    remote = _remote_columns(target, columns[:-1])
    table = target.__table__
    return sa.ForeignKeyConstraint(
        list(columns), [table.c[name] for name in (*remote, "version")], **kwargs
    )


def latest_relationship(
    target: type[AuditableMixin], *columns: str, uselist: bool = False, **kwargs: Any
) -> Any:
    """A relationship resolving to the newest version of ``target``.

    ``columns`` names the local columns mirroring the entity key -- no
    version column, and no foreign key, since the primary key of ``target``
    holds a version this child deliberately does not pin::

        class Tag(UUIDModelMixin, Base):
            article_id: Mapped[UUID] = mapped_column(GUID())

            article = latest_relationship(Article, "article_id")

    The join carries :meth:`~sqlargon.mixins.AuditableMixin.is_latest`, so the
    child follows the entity forward as versions are appended. Having no
    foreign key to write back through, it is necessarily ``viewonly``.
    """
    remote = _remote_columns(target, columns)

    # declared_attr only to reach the class being declared: the join needs its
    # columns, and a helper called from a class body has no other handle on it
    @declared_attr
    def _latest(cls: Any) -> Relationship[Any]:
        local = [getattr(cls, name) for name in columns]
        return relationship(
            target,
            primaryjoin=sa.and_(
                *(
                    column == getattr(target, name)
                    for column, name in zip(local, remote, strict=True)
                ),
                target.is_latest(),
            ),
            foreign_keys=local,
            viewonly=True,
            uselist=uselist,
            **kwargs,
        )

    return _latest
