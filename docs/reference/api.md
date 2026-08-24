# API reference

Generated from the source. See [Usage](../usage.md) for a narrative introduction.

## Repository

::: sqlargon.repository.SQLAlchemyRepository

::: sqlargon.repository.SoftDeleteRepository

::: sqlargon.repository.VersionedRepository

::: sqlargon.repository.AuditableRepository

::: sqlargon.repository.DeletedRowExistsError

::: sqlargon.repository.ConcurrentModificationError

::: sqlargon.repository.AppendOnlyError

::: sqlargon.functools.atomic

## Unit of work

::: sqlargon.uow.SQLAlchemyUnitOfWork

::: sqlargon.uow.AbstractUnitOfWork

## Databases

::: sqlargon.database.BaseDatabase

::: sqlargon.database.Database

::: sqlargon.database.ReadOnlyDatabase

::: sqlargon.database.ReadOnlyError

::: sqlargon.cluster.DatabaseCluster

::: sqlargon.registry.get_default_database

::: sqlargon.registry.set_default_database

## Routing

::: sqlargon.routing.Router

::: sqlargon.routing.RoutingContext

::: sqlargon.routing.RoutingOptions

::: sqlargon.routing.RoutingError

::: sqlargon.routing.using

::: sqlargon.routing.UsingContext

::: sqlargon.routing.use_context

::: sqlargon.routing.read_only

::: sqlargon.routing.DefaultRouter

::: sqlargon.routing.PrimaryReplicaRouter

::: sqlargon.routing.ModelRouter

::: sqlargon.routing.ShardRouter

## Query builder

::: sqlargon.query_builder.QueryBuilder

::: sqlargon.query_builder.Option

::: sqlargon.query_builder.QueryBuilderError

::: sqlargon.query_builder.UnsupportedOption

::: sqlargon.query_builder.get_query_builder

## Pagination

::: sqlargon.pagination.abc.PaginationStrategy

::: sqlargon.pagination.abc.Paginator

::: sqlargon.pagination.abc.SupportsPagination

::: sqlargon.pagination.page.PageNumberPagination

::: sqlargon.pagination.page.TotalPageNumberPagination

::: sqlargon.pagination.offset_limit.LimitOffsetPagination

::: sqlargon.pagination.offset_limit.TotalLimitOffsetPagination

::: sqlargon.pagination.cursor.CursorPagination

::: sqlargon.pagination.models

## Cron

::: sqlargon.cron.Cron

::: sqlargon.cron.CronTask

::: sqlargon.cron.CronTaskRepository

::: sqlargon.cron.utils.validate_schedule

::: sqlargon.cron.utils.next_run_time

## Outbox

::: sqlargon.outbox.OutboxRepository

::: sqlargon.outbox.OutboxEventRepository

::: sqlargon.outbox.OutboxRelay

::: sqlargon.outbox.OutboxEvent

::: sqlargon.outbox.OutboxConfig

::: sqlargon.outbox.Operation

::: sqlargon.outbox.format_topic

::: sqlargon.integrations.eventiq.to_cloud_event

::: sqlargon.integrations.eventiq.eventiq_publisher

## Vector search

::: sqlargon.vectors.VectorRepository

::: sqlargon.vectors.TextSearchRepository

::: sqlargon.vectors.HybridVectorRepository

::: sqlargon.vectors.VectorCollectionRepository

::: sqlargon.vectors.EmbeddingMixin

::: sqlargon.vectors.TextMixin

::: sqlargon.vectors.AttributesMixin

::: sqlargon.vectors.VectorCollectionMixin

::: sqlargon.types.vector.Vector

::: sqlargon.types.vector.DistanceMetric

::: sqlargon.vectors.init_vectors

::: sqlargon.vectors.register_sqlite_vector

## Internationalization

::: sqlargon.i18n.TranslatedRepository

::: sqlargon.i18n.TranslatableMixin

::: sqlargon.i18n.TranslatableBase

::: sqlargon.i18n.TranslationMixin

::: sqlargon.i18n.TranslatedString

::: sqlargon.i18n.Translation

::: sqlargon.i18n.translation_table

::: sqlargon.i18n.translation_class

::: sqlargon.i18n.current_translation

::: sqlargon.i18n.set_locale_getter

::: sqlargon.i18n.get_locale

::: sqlargon.i18n.set_fallback_chain

::: sqlargon.i18n.fallback_chain

::: sqlargon.i18n.select_current

::: sqlargon.i18n.as_translation

## ORM and types

::: sqlargon.orm.Base

::: sqlargon.mixins

::: sqlargon.audit

::: sqlargon.types.uuid

::: sqlargon.types.datetime

::: sqlargon.types.json.JSON

::: sqlargon.types.pydantic

## Settings

::: sqlargon.settings.DatabaseSettings

::: sqlargon.settings.DatabaseClusterSettings

## Typing helpers

::: sqlargon.typing
