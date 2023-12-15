package org.sunbird.obsrv.pipeline.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class MergedPipelineConfig(override val config: Config) extends BaseJobConfig[mutable.Map[String, AnyRef]](config, "MergedPipelineJob") {

  private val serialVersionUID = 2905979434303791379L
  implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  // Kafka Topics Configuration
  override def inputTopic(): String = config.getString("kafka.input.topic")

  override def inputConsumer(): String = "pipeline-consumer"

  override def successTag(): OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("processing_stats")

  override def failedEventsOutputTag(): OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("failed-events")
}
