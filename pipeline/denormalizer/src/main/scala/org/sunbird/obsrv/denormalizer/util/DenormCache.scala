package org.sunbird.obsrv.denormalizer.util

import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.util.{JSONUtil, Util}
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.model.DatasetModels.{Dataset, DenormFieldConfig}
import redis.clients.jedis.{Pipeline, Response}

import scala.collection.mutable

case class DenormFieldStatus(fieldValue: String, var success: Boolean, var error: Option[Error])

case class DenormEvent(msg: mutable.Map[String, AnyRef], var responses: Option[mutable.Map[String, Response[String]]] = None, var fieldStatus: mutable.Map[String, DenormFieldStatus] = mutable.Map[String, DenormFieldStatus]())

class DenormCache(val config: DenormalizerConfig) {

  private val datasetPipelineMap: mutable.Map[String, Pipeline] = mutable.Map[String, Pipeline]()

  def close(): Unit = {
    datasetPipelineMap.values.foreach(pipeline => pipeline.close())
  }

  def open(datasets: List[Dataset]): Unit = {
    datasets.map(dataset => {
      open(dataset)
    })
  }

  def open(dataset: Dataset): Unit = {
    if (!datasetPipelineMap.contains(dataset.id) && dataset.denormConfig.isDefined) {
      val denormConfig = dataset.denormConfig.get
      val redisConnect = new RedisConnect(denormConfig.redisDBHost, denormConfig.redisDBPort, config.redisConnectionTimeout)
      val pipeline: Pipeline = redisConnect.getConnection(0).pipelined()
      datasetPipelineMap.put(dataset.id, pipeline)
    }
  }

  private def processDenorm(denormEvent: DenormEvent, pipeline: Pipeline, denormFieldConfigs: List[DenormFieldConfig]): Unit = {

    val responses: mutable.Map[String, Response[String]] = mutable.Map[String, Response[String]]()
    val fieldStatus: mutable.Map[String, DenormFieldStatus] = mutable.Map[String, DenormFieldStatus]()
    val event = Util.getMutableMap(denormEvent.msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]])
    val eventStr = JSONUtil.serialize(event)
    denormFieldConfigs.foreach(fieldConfig => {
      val denormFieldStatus = extractField(fieldConfig, eventStr)
      fieldStatus.put(fieldConfig.denormOutField, denormFieldStatus)
      if (!denormFieldStatus.fieldValue.isBlank) {
        responses.put(fieldConfig.denormOutField, getFromCache(pipeline, denormFieldStatus.fieldValue, fieldConfig))
      }
    })
    denormEvent.fieldStatus = fieldStatus
    denormEvent.responses = Some(responses)
  }

  def denormEvent(datasetId: String, denormEvent: DenormEvent, denormFieldConfigs: List[DenormFieldConfig]): DenormEvent = {
    val pipeline = this.datasetPipelineMap(datasetId)
    pipeline.clear()
    processDenorm(denormEvent, pipeline, denormFieldConfigs)
    pipeline.sync()
    updateEvent(denormEvent)
  }

  def denormMultipleEvents(datasetId: String, events: List[DenormEvent], denormFieldConfigs: List[DenormFieldConfig]): List[DenormEvent] = {
    val pipeline = this.datasetPipelineMap(datasetId)
    pipeline.clear()

    events.foreach(denormEvent => {
      processDenorm(denormEvent, pipeline, denormFieldConfigs)
    })

    pipeline.sync()
    updateMultipleEvents(events)
  }

  private def extractField(fieldConfig: DenormFieldConfig, eventStr: String): DenormFieldStatus = {
    val denormFieldNode = JSONUtil.getKey(fieldConfig.denormKey, eventStr)
    if (denormFieldNode.isMissingNode) {
      DenormFieldStatus("", success = false, Some(ErrorConstants.DENORM_KEY_MISSING))
    } else {
      if (denormFieldNode.isTextual || denormFieldNode.isNumber) {
        DenormFieldStatus(denormFieldNode.asText(), success = false, None)
      } else {
        DenormFieldStatus("", success = false, Some(ErrorConstants.DENORM_KEY_NOT_A_STRING_OR_NUMBER))
      }
    }
  }

  private def getFromCache(pipeline: Pipeline, denormField: String, fieldConfig: DenormFieldConfig): Response[String] = {
    pipeline.select(fieldConfig.redisDB)
    pipeline.get(denormField)
  }

  private def updateEvent(denormEvent: DenormEvent): DenormEvent = {

    val event = Util.getMutableMap(denormEvent.msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]])
    denormEvent.responses.get.foreach(f => {
      if (f._2.get() != null) {
        denormEvent.fieldStatus(f._1).success = true
        event.put(f._1, JSONUtil.deserialize[Map[String, AnyRef]](f._2.get()))
      } else {
        denormEvent.fieldStatus(f._1).error = Some(ErrorConstants.DENORM_DATA_NOT_FOUND)
      }
    })
    denormEvent.msg.put(config.CONST_EVENT, event.toMap)
    denormEvent
  }

  private def updateMultipleEvents(events: List[DenormEvent]): List[DenormEvent] = {

    events.map(denormEvent => {
      updateEvent(denormEvent)
    })
  }
}