package org.sunbird.obsrv.pipeline.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class MasterDataProcessorConfig(override val config: Config) extends BaseJobConfig[mutable.Map[String, AnyRef]](config, "MasterDataProcessorJob") {

  private val serialVersionUID = 2905979434303791379L
  implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  // Kafka Topics Configuration
  val kafkaStatsTopic: String = config.getString("kafka.stats.topic")
  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")

  // Metric List
  val totalEventCount = "total-event-count"
  val successEventCount = "success-event-count"
  val successInsertCount = "success-insert-count"
  val successUpdateCount = "success-update-count"
  val failedCount = "event-failed-count"

  val windowTime: Int = config.getInt("task.window.time.in.seconds")
  val windowCount: Int = config.getInt("task.window.count")

  val failedEventsTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("failed_events")
  private val statsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("processing_stats")

  // Functions
  val masterDataProcessFunction = "MasterDataProcessorFunction"
  val failedEventsProducer = "MasterDataFailedEventsProducer"

  override def inputTopic(): String = config.getString("kafka.input.topic")
  override def inputConsumer(): String = "master-data-consumer"
  override def successTag(): OutputTag[mutable.Map[String, AnyRef]] = statsOutputTag
}
