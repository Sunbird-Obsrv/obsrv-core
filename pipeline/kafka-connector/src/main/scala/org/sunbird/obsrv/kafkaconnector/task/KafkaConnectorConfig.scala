package org.sunbird.obsrv.kafkaconnector.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class KafkaConnectorConfig (override val config: Config) extends BaseJobConfig[String](config, "KafkaConnectorJob") {

  private val serialVersionUID = 2905979435603791379L

  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  // val kafkaOutputTopic: String = config.getString("kafka.output.topic")


  override def inputTopic(): String = ""
  override def inputConsumer(): String = ""

  private val DUMMY_OUTPUT_TAG = "dummy-events"
  override def successTag(): OutputTag[String] = OutputTag[String](DUMMY_OUTPUT_TAG)

}
