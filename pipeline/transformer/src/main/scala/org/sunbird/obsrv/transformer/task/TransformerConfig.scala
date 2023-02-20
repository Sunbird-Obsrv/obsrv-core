package org.sunbird.obsrv.transformer.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class TransformerConfig(override val config: Config) extends BaseJobConfig(config, "TransformerJob") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  // Metric List
  val totalEventCount = "total-event-count"
  val transformSuccessCount = "transform-success-count"
  val transformFailedCount = "transform-failed-count"
  val transformSkippedCount = "transform-skipped-count"

  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaTransformTopic: String = config.getString("kafka.output.transform.topic")

  val transformerConsumer = "transformer-consumer"
  val transformerFunction = "transformer-function"
  val transformerProducer = "transformer-producer"

  private val TRANSFORMER_OUTPUT_TAG = "transformed-events"
  val transformerOutputTag: OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]](TRANSFORMER_OUTPUT_TAG)

}
