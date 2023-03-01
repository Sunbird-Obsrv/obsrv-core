package org.sunbird.spec

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable
import scala.collection.mutable.Map

class BaseProcessTestConfig(override val config: Config) extends BaseJobConfig[String](config, "Test-job") {
  private val serialVersionUID = -2349318979085017498L
  implicit val mapTypeInfo: TypeInformation[Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[Map[String, AnyRef]])

  val mapOutputTag: OutputTag[Map[String, AnyRef]] = OutputTag[Map[String, AnyRef]]("test-map-stream-tag")
  val stringOutputTag: OutputTag[String] = OutputTag[String]("test-string-stream-tag")

  val kafkaMapInputTopic: String = config.getString("kafka.map.input.topic")
  val kafkaMapOutputTopic: String = config.getString("kafka.map.output.topic")
  val kafkaEventInputTopic: String = config.getString("kafka.event.input.topic")
  val kafkaEventOutputTopic: String = config.getString("kafka.event.output.topic")
  val kafkaEventDuplicateTopic: String = config.getString("kafka.event.duplicate.topic")
  val kafkaStringInputTopic: String = config.getString("kafka.string.input.topic")
  val kafkaStringOutputTopic: String = config.getString("kafka.string.output.topic")

  val testTopics = List(kafkaMapInputTopic, kafkaMapOutputTopic, kafkaEventInputTopic, kafkaEventOutputTopic,
    kafkaStringInputTopic, kafkaStringOutputTopic)

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  val dedupStore: Int = config.getInt("redis.database.duplicationstore.id")
  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")
  val mapEventCount = "map-event-count"
  val eventCount = "event-count"
  val stringEventCount = "string-event-count"

  override def inputTopic(): String = kafkaMapInputTopic

  override def inputConsumer(): String = "testConsumer"

  override def successTag(): OutputTag[String] = stringOutputTag
}

class BaseProcessTestMapConfig(override val config: Config) extends BaseJobConfig[Map[String, AnyRef]](config, "Test-job") {
  private val serialVersionUID = -2349318979085017498L
  implicit val mapTypeInfo: TypeInformation[Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[Map[String, AnyRef]])

  val mapOutputTag: OutputTag[Map[String, AnyRef]] = OutputTag[Map[String, AnyRef]]("test-map-stream-tag")
  val stringOutputTag: OutputTag[String] = OutputTag[String]("test-string-stream-tag")

  val kafkaMapInputTopic: String = config.getString("kafka.map.input.topic")
  val kafkaMapOutputTopic: String = config.getString("kafka.map.output.topic")
  val kafkaEventInputTopic: String = config.getString("kafka.event.input.topic")
  val kafkaEventOutputTopic: String = config.getString("kafka.event.output.topic")
  val kafkaEventDuplicateTopic: String = config.getString("kafka.event.duplicate.topic")
  val kafkaStringInputTopic: String = config.getString("kafka.string.input.topic")
  val kafkaStringOutputTopic: String = config.getString("kafka.string.output.topic")

  val testTopics = List(kafkaMapInputTopic, kafkaMapOutputTopic, kafkaEventInputTopic, kafkaEventOutputTopic,
    kafkaStringInputTopic, kafkaStringOutputTopic)

  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  val dedupStore: Int = config.getInt("redis.database.duplicationstore.id")
  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")
  val mapEventCount = "map-event-count"
  val telemetryEventCount = "telemetry-event-count"
  val stringEventCount = "string-event-count"

  override def inputTopic(): String = kafkaMapInputTopic

  override def inputConsumer(): String = "testConsumer"

  override def successTag(): OutputTag[Map[String, AnyRef]] = mapOutputTag
}