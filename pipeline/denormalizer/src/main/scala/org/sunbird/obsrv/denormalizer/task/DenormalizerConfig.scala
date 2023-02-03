package org.sunbird.obsrv.denormalizer.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig
import scala.collection.mutable

class DenormalizerConfig(override val config: Config, jobName: String) extends BaseJobConfig(config, jobName ) {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  implicit val anyTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")
  val denormOutputTopic: String = config.getString("kafka.denorm.output.topic")
  val denormFailedTopic: String = config.getString("kafka.denorm.failed.topic")

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  val downstreamOperatorsParallelism: Int = config.getInt("task.downstream.operators.parallelism")

  // Windows
  val windowTime: Int = config.getInt("task.window.time.in.seconds")
  val windowCount: Int = config.getInt("task.window.count")

  val DENORM_EVENTS_PRODUCER = "telemetry-denorm-events-producer"

  private val DENORM_EVENTS = "denorm_events"
  private val FAILED_EVENTS = "denorm_failed_events"

  val denormEventsTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](DENORM_EVENTS)
  val denormFailedTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](FAILED_EVENTS)

  val eventsSkipped = "events-skipped"
  val denormFailed = "denorm-failed"
  val denormSuccess = "denorm-success"
  val denormTotal = "denorm-total"

  // Consumers
  val denormalizationConsumer = "denormalization-consumer"

  // Functions
  val denormalizationFunction = "DenormalizationFunction"

}
