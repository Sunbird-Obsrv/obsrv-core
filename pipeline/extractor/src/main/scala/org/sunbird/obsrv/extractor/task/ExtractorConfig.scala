package org.sunbird.obsrv.extractor.task

import scala.collection.mutable
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.model.SystemConfig
import org.sunbird.obsrv.core.streaming.BaseJobConfig

class ExtractorConfig(override val config: Config) extends BaseJobConfig(config, "ExtractorJob") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val dedupStore: Int = config.getInt("redis.database.extractionduplicates.id")
  val cacheExpiryInSeconds: Int = SystemConfig.defaultDedupPeriodInSeconds

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaSuccessTopic: String = config.getString("kafka.output.success.topic")
  val kafkaDuplicateTopic: String = config.getString("kafka.output.duplicate.topic")
  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaBatchFailedTopic: String = config.getString("kafka.output.batch.failed.topic")
  val eventMaxSize: Long = SystemConfig.maxEventSize

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val downstreamOperatorsParallelism: Int = config.getInt("task.downstream.operators.parallelism")

  private val RAW_EVENTS_OUTPUT_TAG = "raw-events"
  private val FAILED_EVENTS_OUTPUT_TAG = "failed-events"
  private val FAILED_BATCH_EVENTS_OUTPUT_TAG = "failed-batch-events"
  private val DUPLICATE_EVENTS_OUTPUT_TAG = "duplicate-batch-events"

  // Metric List
  val successEventCount = "success-event-count"
  val failedEventCount = "failed-event-count"
  val failedExtractionCount = "failed-extraction-count"
  val successExtractionCount = "success-extraction-count"
  val duplicateExtractionCount = "duplicate-extraction-count"
  val skippedExtractionCount = "skipped-extraction-count"

  val rawEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](RAW_EVENTS_OUTPUT_TAG)
  val failedEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](FAILED_EVENTS_OUTPUT_TAG)
  val failedBatchEventOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](FAILED_BATCH_EVENTS_OUTPUT_TAG)
  val duplicateEventOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](id = DUPLICATE_EVENTS_OUTPUT_TAG)

  // Consumers
  val extractorConsumer = "extractor-consumer"

  // Functions
  val extractionFunction = "ExtractionFunction"

  // Producers
  val extractorDuplicateProducer = "extractor-duplicate-events-sink"
  val extractorBatchFailedEventsProducer = "extractor-batch-failed-events-sink"
  val extractorRawEventsProducer = "extractor-raw-events-sink"
  val extractorSystemEventsProducer = "extractor-system-events-sink"
  val extractorFailedEventsProducer = "extractor-failed-events-sink"
}
