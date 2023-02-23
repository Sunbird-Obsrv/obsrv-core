package org.sunbird.obsrv.preprocessor.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class PipelinePreprocessorConfig(override val config: Config) extends BaseJobConfig[mutable.Map[String, AnyRef]](config, "PipelinePreprocessorJob") {

  private val serialVersionUID = 2905979434303791379L

  implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val dedupStore: Int = config.getInt("redis.database.preprocessor.duplication.store.id")
  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")

  // Kafka Topic Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaInvalidTopic: String = config.getString("kafka.output.invalid.topic")
  val kafkaUniqueTopic: String = config.getString("kafka.output.unique.topic")
  val kafkaDuplicateTopic: String = config.getString("kafka.output.duplicate.topic")

  // Validation & dedup Stream out put tag
  val failedEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("failed-events")
  val invalidEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("invalid-events")
  val validEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("valid-events")
  val uniqueEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("unique-events")
  val duplicateEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("duplicate-events")

  // Validation job metrics
  val validationTotalMetricsCount = "validation-total-event-count"
  val validationSuccessMetricsCount = "validation-success-event-count"
  val validationFailureMetricsCount = "validation-failed-event-count"
  val eventFailedMetricsCount = "failed-event-count"
  val validationSkipMetricsCount = "validation-skipped-event-count"

  val duplicationTotalMetricsCount = "duplicate-total-count"
  val duplicationEventMetricsCount = "duplicate-event-count"
  val duplicationSkippedEventMetricsCount = "duplicate-skipped-event-count"
  val duplicationProcessedEventMetricsCount = "duplicate-processed-event-count"

  // Consumers
  val validationConsumer = "validation-consumer"
  val dedupConsumer = "deduplication-consumer"

  // Producers
  val failedEventProducer = "failed-events-sink"
  val invalidEventProducer = "invalid-events-sink"
  val duplicateEventProducer = "duplicate-events-sink"
  val uniqueEventProducer = "unique-events-sink"

  override def inputTopic(): String = kafkaInputTopic

  override def inputConsumer(): String = validationConsumer

  override def successTag(): OutputTag[mutable.Map[String, AnyRef]] = uniqueEventsOutputTag
}
