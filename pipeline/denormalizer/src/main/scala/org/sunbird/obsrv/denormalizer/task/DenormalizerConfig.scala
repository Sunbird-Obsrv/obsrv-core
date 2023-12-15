package org.sunbird.obsrv.denormalizer.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class DenormalizerConfig(override val config: Config) extends BaseJobConfig[mutable.Map[String, AnyRef]](config, "DenormalizerJob") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  implicit val anyTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val denormOutputTopic: String = config.getString("kafka.output.denorm.topic")

  // Windows
  val windowTime: Int = config.getInt("task.window.time.in.seconds")
  val windowCount: Int = config.getInt("task.window.count")

  val DENORM_EVENTS_PRODUCER = "denorm-events-producer"

  private val DENORM_EVENTS = "denorm_events"

  val denormEventsTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](DENORM_EVENTS)

  val eventsSkipped = "denorm-skipped"
  val denormFailed = "denorm-failed"
  val denormPartialSuccess = "denorm-partial-success"
  val denormSuccess = "denorm-success"
  val denormTotal = "denorm-total"

  // Consumers
  val denormalizationConsumer = "denormalization-consumer"

  // Functions
  val denormalizationFunction = "DenormalizationFunction"

  override def inputTopic(): String = kafkaInputTopic
  override def inputConsumer(): String = denormalizationConsumer
  override def successTag(): OutputTag[mutable.Map[String, AnyRef]] = denormEventsTag
  override def failedEventsOutputTag(): OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("failed-events")

}