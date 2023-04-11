package org.sunbird.obsrv.kafkaconnector.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class KafkaConnectorConfig (override val config: Config) extends BaseJobConfig[mutable.Map[String, AnyRef]](config, "KafkaConnectorJob") {

  private val serialVersionUID = 2905979435603791379L

  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaOutputTopic: String = config.getString("kafka.output.topic")
  private val RAW_EVENTS_OUTPUT_TAG = "raw-events"
  val rawEventsOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](RAW_EVENTS_OUTPUT_TAG)

  val totalEventCount = "total-event-count"
  val successEventCount = "success-event-count"
  val failedEventCount = "failed-event-count"

  override def inputTopic(): String = kafkaInputTopic

  override def inputConsumer(): String = "kafka-connector-consumer"

  override def successTag(): OutputTag[mutable.Map[String, AnyRef]] = rawEventsOutputTag

}
