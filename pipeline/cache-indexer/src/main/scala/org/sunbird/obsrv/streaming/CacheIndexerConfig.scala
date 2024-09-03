package org.sunbird.obsrv.pipeline.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable

class CacheIndexerConfig(override val config: Config) extends BaseJobConfig[mutable.Map[String, AnyRef]](config, "CacheIndexerJob") {

  private val serialVersionUID = 2905979434303791379L
  implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  // Metric List
  val totalEventCount = "total-event-count"
  val successEventCount = "success-event-count"
  val successInsertCount = "success-insert-count"
  val successUpdateCount = "success-update-count"

  val windowTime: Int = config.getInt("task.window.time.in.seconds")
  val windowCount: Int = config.getInt("task.window.count")

  // Functions
  val cacheIndexerFunction = "CacheIndexerFunction"

  override def inputTopic(): String = null
  override def inputConsumer(): String = "cache-indexer"
  override def successTag(): OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("processing_stats")

  override def failedEventsOutputTag(): OutputTag[mutable.Map[String, AnyRef]] = OutputTag[mutable.Map[String, AnyRef]]("failed-events")
}
