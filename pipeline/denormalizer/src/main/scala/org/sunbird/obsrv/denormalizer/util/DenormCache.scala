package org.sunbird.obsrv.denormalizer.util

import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.util.{JSONUtil, Util}
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.model.DatasetModels.{Dataset, DenormFieldConfig}

import scala.collection.mutable
import redis.clients.jedis.Pipeline
import redis.clients.jedis.Response

case class DenormEvent(msg: mutable.Map[String, AnyRef], var responses: Option[mutable.Map[String, Response[String]]], var error: Option[Error])

class DenormCache(val config: DenormalizerConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormCache])
  private val datasetPipelineMap: mutable.Map[String, Pipeline] = mutable.Map[String, Pipeline]()

  def close(): Unit = {
    datasetPipelineMap.values.foreach(pipeline => pipeline.close())
  }

  def open(datasets: List[Dataset]): Unit = {
    datasets.map(dataset => {
      if (dataset.denormConfig.isDefined) {
        val denormConfig = dataset.denormConfig.get
        val redisConnect = new RedisConnect(denormConfig.redisDBHost, denormConfig.redisDBPort, config)
        val pipeline: Pipeline = redisConnect.getConnection(0).pipelined()
        datasetPipelineMap.put(dataset.id, pipeline)
      }
    })
  }

  def denormEvent(datasetId: String, event: mutable.Map[String, AnyRef], denormFieldConfigs: List[DenormFieldConfig]):mutable.Map[String, AnyRef] = {
    val pipeline = this.datasetPipelineMap(datasetId)
    pipeline.clear()
    val responses : mutable.Map[String, Response[String]] = mutable.Map[String, Response[String]]()
    val eventStr = JSONUtil.serialize(event)
    denormFieldConfigs.foreach(fieldConfig => {
      responses.put(fieldConfig.denormOutField, getFromCache(pipeline, fieldConfig, eventStr))
    })
    pipeline.sync()
    updateEvent(event, responses)
  }

  def denormMultipleEvents(datasetId: String, events: List[DenormEvent], denormFieldConfigs: List[DenormFieldConfig]): List[DenormEvent] = {
    val pipeline = this.datasetPipelineMap(datasetId)
    pipeline.clear()

    events.foreach(denormEvent => {
      val responses: mutable.Map[String, Response[String]] = mutable.Map[String, Response[String]]()
      val event = Util.getMutableMap(denormEvent.msg("event").asInstanceOf[Map[String, AnyRef]])
      val eventStr = JSONUtil.serialize(event)
      try {
        denormFieldConfigs.foreach(fieldConfig => {
          responses.put(fieldConfig.denormOutField, getFromCache(pipeline, fieldConfig, eventStr))
        })
        denormEvent.responses = Some(responses)
      } catch {
        case ex: ObsrvException =>
          denormEvent.error = Some(ex.error)
      }
    })

    pipeline.sync()
    updateMultipleEvents(events)
  }

  private def getFromCache(pipeline: Pipeline, fieldConfig: DenormFieldConfig, eventStr: String): Response[String] = {
    pipeline.select(fieldConfig.redisDB)
    val denormFieldNode = JSONUtil.getKey(fieldConfig.denormKey, eventStr)
    if(denormFieldNode.isMissingNode) {
      throw new ObsrvException(ErrorConstants.DENORM_KEY_MISSING)
    }
    if(!denormFieldNode.isTextual) {
      throw new ObsrvException(ErrorConstants.DENORM_KEY_NOT_A_STRING)
    }
    val denormField = denormFieldNode.asText()
    pipeline.get(denormField)
  }

  private def updateEvent(event: mutable.Map[String, AnyRef], responses: mutable.Map[String, Response[String]]): mutable.Map[String, AnyRef] = {

    responses.map(f => {
      event.put(f._1, JSONUtil.deserialize[Map[String, AnyRef]](f._2.get()))
    })
    event
  }

  private def updateMultipleEvents(events: List[DenormEvent]):List[DenormEvent] = {

    events.map(denormEvent => {
      if(denormEvent.responses.isDefined) {
        val event = Util.getMutableMap(denormEvent.msg("event").asInstanceOf[Map[String, AnyRef]])
        denormEvent.responses.get.map(f => {
          event.put(f._1, JSONUtil.deserialize[Map[String, AnyRef]](f._2.get()))
        })
        denormEvent.msg.put("event", event)
      }
      denormEvent
    })
  }

}

// $COVERAGE-ON$
