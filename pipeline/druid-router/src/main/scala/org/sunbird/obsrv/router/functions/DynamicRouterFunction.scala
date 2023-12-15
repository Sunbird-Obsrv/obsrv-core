package org.sunbird.obsrv.router.functions

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.{Constants, ErrorConstants, FunctionalError, Producer}
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.{JSONUtil, Util}
import org.sunbird.obsrv.model.DatasetModels.{Dataset, DatasetConfig}
import org.sunbird.obsrv.router.task.DruidRouterConfig
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction

import java.util.TimeZone
import scala.collection.mutable

case class TimestampKey(isValid: Boolean, value: AnyRef)

class DynamicRouterFunction(config: DruidRouterConfig) extends BaseDatasetProcessFunction(config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DynamicRouterFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def getMetrics(): List[String] = {
    List(config.routerTotalCount, config.routerSuccessCount)
  }

  override def processElement(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                              ctx: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(dataset.id, config.routerTotalCount)
    val event = Util.getMutableMap(msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]])
    val tsKeyData = TimestampKeyParser.parseTimestampKey(dataset.datasetConfig, event)
    if (tsKeyData.isValid) {
      event.put(config.CONST_OBSRV_META, msg(config.CONST_OBSRV_META).asInstanceOf[Map[String, AnyRef]] ++ Map("indexTS" -> tsKeyData.value))
      val routerConfig = dataset.routerConfig
      val topicEventMap = mutable.Map(Constants.TOPIC -> routerConfig.topic, Constants.MESSAGE -> event)
      ctx.output(config.routerOutputTag, topicEventMap)
      metrics.incCounter(dataset.id, config.routerSuccessCount)
      markCompletion(dataset, super.markComplete(event, dataset.dataVersion), ctx, Producer.router)
    } else {
      markFailure(Some(dataset.id), msg, ctx, metrics, ErrorConstants.INDEX_KEY_MISSING_OR_BLANK, Producer.router, FunctionalError.MissingTimestampKey)
    }
  }

}

object TimestampKeyParser {

  def parseTimestampKey(datasetConfig: DatasetConfig, event: mutable.Map[String, AnyRef]): TimestampKey = {
    val indexKey = datasetConfig.tsKey
    val node = JSONUtil.getKey(indexKey, JSONUtil.serialize(event))
    node.getNodeType match {
      case JsonNodeType.NUMBER => onNumber(datasetConfig, node)
      case JsonNodeType.STRING => onText(datasetConfig, node)
      case _ => TimestampKey(isValid = false, null)
    }
  }

  private def onNumber(datasetConfig: DatasetConfig, node: JsonNode): TimestampKey = {
    val length = node.asText().length
    val value = node.numberValue().longValue()
    // TODO: [P3] Crude implementation. Checking if the epoch timestamp format is one of seconds, milli-seconds, micro-second and nano-seconds. Find a elegant approach
    if (length == 10 || length == 13 || length == 16 || length == 19) {
      val tfValue:Long = if (length == 10) (value * 1000).longValue() else if (length == 16) (value / 1000).longValue() else if (length == 19) (value / 1000000).longValue() else value
      TimestampKey(isValid = true, addTimeZone(datasetConfig, new DateTime(tfValue)).asInstanceOf[AnyRef])
    } else {
      TimestampKey(isValid = false, 0.asInstanceOf[AnyRef])
    }
  }

  private def onText(datasetConfig: DatasetConfig, node: JsonNode): TimestampKey = {
    val value = node.textValue()
    if (datasetConfig.tsFormat.isDefined) {
      parseDateTime(datasetConfig, value)
    } else {
      TimestampKey(isValid = true, value)
    }
  }

  private def parseDateTime(datasetConfig: DatasetConfig, value: String): TimestampKey = {
    try {
      datasetConfig.tsFormat.get match {
        case "epoch" => TimestampKey(isValid = true, addTimeZone(datasetConfig, new DateTime(value.toLong)).asInstanceOf[AnyRef])
        case _ =>
          val dtf = DateTimeFormat.forPattern(datasetConfig.tsFormat.get)
          TimestampKey(isValid = true, addTimeZone(datasetConfig, dtf.parseDateTime(value)).asInstanceOf[AnyRef])
      }
    } catch {
      case _: Exception => TimestampKey(isValid = false, null)
    }
  }

  private def addTimeZone(datasetConfig: DatasetConfig, dateTime: DateTime): Long = {
    if (datasetConfig.datasetTimezone.isDefined) {
      val tz = DateTimeZone.forTimeZone(TimeZone.getTimeZone(datasetConfig.datasetTimezone.get))
      val offsetInMilliseconds = tz.getOffset(dateTime)
      dateTime.plusMillis(offsetInMilliseconds).getMillis
    } else {
      dateTime.getMillis
    }
  }

}