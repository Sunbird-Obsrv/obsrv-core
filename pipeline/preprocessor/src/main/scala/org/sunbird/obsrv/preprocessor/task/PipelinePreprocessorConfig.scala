package org.sunbird.obsrv.preprocessor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class PipelinePreprocessorConfig(override val config: Config) extends BaseJobConfig(config, "PipelinePreprocessorJob") {

  private val serialVersionUID = 2905979434303791379L

  implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val schemaPath: String = config.getString("telemetry.schema.path")

  val dedupStore: Int = config.getInt("redis.database.duplicationstore.id")
  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")

  // Kafka Topic Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")

  val kafkaPrimaryRouteTopic: String = config.getString("kafka.output.primary.route.topic")
  val kafkaLogRouteTopic: String = config.getString("kafka.output.log.route.topic")
  val kafkaErrorRouteTopic: String = config.getString("kafka.output.error.route.topic")
  val kafkaAuditRouteTopic: String = config.getString("kafka.output.audit.route.topic")

  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaInvalidTopic: String = config.getString("kafka.output.invalid.topic")
  val kafkaDuplicateTopic: String = config.getString("kafka.output.duplicate.topic")

  val kafkaDenormSecondaryRouteTopic: String = config.getString("kafka.output.denorm.secondary.route.topic")
  val kafkaDenormPrimaryRouteTopic: String = config.getString("kafka.output.denorm.primary.route.topic")

  val defaultChannel: String = config.getString("default.channel")

  // Validation & dedup Stream out put tag
  val failedEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("failed-events")
  val invalidEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("invalid-events")
  val validEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("valid-events")
  val uniqueEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("unique-events")
  val duplicateEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("duplicate-events")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val downstreamOperatorsParallelism: Int = config.getInt("task.downstream.operators.parallelism")

  val VALIDATION_FLAG_NAME = "pp_validation_processed"
  val DEDUP_FLAG_NAME = "pp_duplicate"
  val DEDUP_SKIP_FLAG_NAME = "pp_duplicate_skipped"
  val SHARE_EVENTS_FLATTEN_FLAG_NAME = "pp_share_event_processed"

  // Router job metrics
  val primaryRouterMetricCount = "primary-route-success-count"
  val auditEventRouterMetricCount = "audit-route-success-count"
  val shareEventsRouterMetricCount = "share-route-success-count"
  val logEventsRouterMetricsCount = "log-route-success-count"
  val errorEventsRouterMetricsCount = "error-route-success-count"
  val denormSecondaryEventsRouterMetricsCount = "denorm-secondary-route-success-count"
  val denormPrimaryEventsRouterMetricsCount = "denorm-primary-route-success-count"

  // Validation job metrics
  val validationSuccessMetricsCount = "validation-success-event-count"
  val validationFailureMetricsCount = "validation-failed-event-count"
  val duplicationEventMetricsCount = "duplicate-event-count"
  val duplicationSkippedEventMetricsCount = "duplicate-skipped-event-count"
  val duplicationProcessedEventMetricsCount = "duplicate-processed-event-count"
  val eventFailedMetricsCount = "failed-event-count"
  val uniqueEventsMetricsCount = "unique-event-count"
  val validationSkipMetricsCount = "validation-skipped-event-count"

  // ShareEventsFlatten count
  val shareItemEventsMetircsCount = "share-item-event-success-count"

  // Consumers
  val validationConsumer = "validation-consumer"
  val dedupConsumer = "deduplication-consumer"

  // Functions
  val telemetryValidationFunction = "TelemetryValidationFunction"
  val telemetryRouterFunction = "TelemetryRouterFunction"
  val shareEventsFlattenerFunction = "ShareEventsFlattenerFunction"

  // Producers
  val primaryRouterProducer = "primary-route-sink"
  val auditEventsPrimaryRouteProducer = "audit-events-primary-route-sink"
  val shareEventsPrimaryRouteProducer = "share-events-primary-route-sink"
  val shareItemsPrimaryRouterProducer = "share-items-primary-route-sink"
  val logRouterProducer = "log-route-sink"
  val errorRouterProducer = "error-route-sink"
  val auditRouterProducer = "audit-route-sink"
  val invalidEventProducer = "invalid-events-sink"
  val duplicateEventProducer = "duplicate-events-sink"
  val denormSecondaryEventProducer = "denorm-secondary-events-sink"
  val denormPrimaryEventProducer = "denorm-primary-events-sink"

  val defaultSchemaFile = "envelope.json"

}
