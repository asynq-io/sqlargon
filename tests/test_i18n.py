"""Unit tests for the locale getter / fallback-chain callable slots
and for ``TranslatedRepository``.

Mirrors ``test_outbox.py``: in-memory SQLite via the shared ``db`` fixture,
module-level models to survive ``--count=3`` re-registration.
"""

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import relationship

import sqlargon.i18n.expression as _expr
import sqlargon.i18n.translation as _trans
from sqlargon import Base, Database
from sqlargon.i18n import (
    TranslatedRepository,
    fallback_chain,
    get_locale,
    set_fallback_chain,
    set_locale_getter,
)

# ── models ──────────────────────────────────────────────────────────────────


class RepoModel(Base):
    __tablename__ = "test_i18n_repo_model"

    id = sa.Column(sa.Integer, primary_key=True)


class RepoTranslation(Base):
    __tablename__ = "test_i18n_repo_trans"

    id = sa.Column(
        sa.Integer,
        sa.ForeignKey("test_i18n_repo_model.id"),
        primary_key=True,
    )


RepoModel._current_translation = relationship(
    RepoTranslation,
    primaryjoin=RepoModel.id == RepoTranslation.id,
    uselist=False,
    viewonly=True,
    lazy="raise",
)


class TestRepo(TranslatedRepository):
    """Concrete repository on a model whose ``_current_translation`` is a
    plain relationship -- not one created by ``TranslatableMixin`` -- which
    is enough to verify the join behaviour.
    """

    __test__ = False
    model = RepoModel


# ── fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_locale_slots():
    """Reset the locale and fallback callable slots before every test."""
    _expr._get_locale = None
    _trans._get_fallback = None


@pytest.fixture(autouse=True)
async def _create_tables(db: Database):
    created = (RepoModel.__table__, RepoTranslation.__table__)
    async with db.engine.begin() as conn:
        for table in created:
            await conn.run_sync(table.create, checkfirst=True)
    yield
    async with db.engine.begin() as conn:
        for table in reversed(created):
            await conn.run_sync(table.drop, checkfirst=True)


# --- locale getter -----------------------------------------------------------


def test_locale_getter_raises_before_configuration():
    with pytest.raises(RuntimeError, match="No locale getter"):
        get_locale()


def test_locale_getter_returns_the_registered_value():
    set_locale_getter(lambda: "pl")

    assert get_locale() == "pl"


def test_locale_getter_replacing_is_honoured():
    set_locale_getter(lambda: "pl")
    set_locale_getter(lambda: "de")

    assert get_locale() == "de"


def test_locale_getter_slot_is_cleared_between_tests():
    """The autouse fixture clears the slot, so a fresh test starts clean."""
    assert _expr._get_locale is None

    set_locale_getter(lambda: "fr")

    assert get_locale() == "fr"


# --- fallback chain ----------------------------------------------------------


def test_fallback_chain_raises_before_configuration():
    with pytest.raises(RuntimeError, match="No fallback chain"):
        fallback_chain()


def test_fallback_chain_returns_the_registered_chain():
    set_fallback_chain(lambda _: ("en", "en-US"))

    assert fallback_chain() == ("en", "en-US")


def test_fallback_chain_passes_the_explicit_locale_through():
    called_with: list[str | None] = []

    def capture(locale: str | None) -> tuple[str, ...]:
        called_with.append(locale)
        return (locale or "en",)

    set_fallback_chain(capture)

    fallback_chain("de")

    assert called_with == ["de"]


def test_fallback_chain_passes_none_when_no_locale_is_given():
    called_with: list[str | None] = []

    def capture(locale: str | None) -> tuple[str, ...]:
        called_with.append(locale)
        return ("en",)

    set_fallback_chain(capture)

    fallback_chain()

    assert called_with == [None]


def test_fallback_chain_replacing_is_honoured():
    set_fallback_chain(lambda _: ("en",))
    set_fallback_chain(lambda _: ("de", "en"))

    assert fallback_chain() == ("de", "en")


def test_fallback_chain_slot_is_cleared_between_tests():
    assert _trans._get_fallback is None

    set_fallback_chain(lambda _: ("fr", "en"))

    assert fallback_chain() == ("fr", "en")


# --- TranslatedRepository ----------------------------------------------------


def test_translated_repository_select_includes_the_outer_join():
    repo = TestRepo()
    stmt = repo.select().query

    compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "LEFT OUTER JOIN" in compiled
    assert "test_i18n_repo_trans" in compiled


def test_translated_repository_select_accepts_column_args():
    repo = TestRepo()
    stmt = repo.select(RepoModel.id).query

    compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    assert "LEFT OUTER JOIN" in compiled
    assert "test_i18n_repo_model" in compiled
