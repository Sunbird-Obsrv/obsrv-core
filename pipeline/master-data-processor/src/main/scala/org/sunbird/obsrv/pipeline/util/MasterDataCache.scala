package org.sunbird.obsrv.pipeline.util

import org.json4s.{DefaultFormats, Formats, JNothing, JValue}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.model.DatasetModels.{Dataset, DatasetConfig}
import org.sunbird.obsrv.pipeline.task.MasterDataProcessorConfig
import redis.clients.jedis.{Pipeline, Response}
import org.json4s.native.JsonMethods._

import scala.collection.mutable

class MasterDataCache(val config: MasterDataProcessorConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MasterDataCache])
  private val datasetPipelineMap: mutable.Map[String, Pipeline] = mutable.Map[String, Pipeline]()

  def close(): Unit = {
    datasetPipelineMap.values.foreach(pipeline => pipeline.close())
  }

  def open(datasets: List[Dataset]): Unit = {
    datasets.map(dataset => {
      val datasetConfig = dataset.datasetConfig
      val redisConnect = new RedisConnect(datasetConfig.redisDBHost.get, datasetConfig.redisDBPort.get, config.redisConnectionTimeout)
      val pipeline: Pipeline = redisConnect.getConnection(0).pipelined()
      datasetPipelineMap.put(dataset.id, pipeline)
    })
  }

  def process(dataset: Dataset, eventMap: Map[String, JValue]): (Int, Int) = {
    val pipeline = this.datasetPipelineMap(dataset.id)
    val dataFromCache = getDataFromCache(dataset, eventMap.keySet, pipeline)
    val insertCount = dataFromCache.filter(f => f._2 == null).size
    val updCount = dataFromCache.size - insertCount
    updateCache(dataset, dataFromCache, eventMap, pipeline)
    (insertCount, updCount)
  }

  private def getDataFromCache(dataset: Dataset, keys: Set[String], pipeline: Pipeline): mutable.Map[String, String] = {
    pipeline.clear()
    pipeline.select(dataset.datasetConfig.redisDB.get)
    val responses: mutable.Map[String, Response[String]] = mutable.Map[String, Response[String]]()
    keys.foreach(key => {
      responses.put(key, pipeline.get(key))
    })
    pipeline.sync()
    responses.map(f => (f._1, f._2.get()))
  }

  private def updateCache(dataset: Dataset, dataFromCache: mutable.Map[String, String], eventMap: Map[String, JValue], pipeline: Pipeline ): Unit = {
    pipeline.clear()
    pipeline.select(dataset.datasetConfig.redisDB.get)
    eventMap.foreach(f => {
      val key = f._1
      val newJson = f._2
      val existingData = dataFromCache(f._1)
      val existingJson = if (existingData != null) parse(existingData) else JNothing
      val mergedJson = existingJson merge newJson
      pipeline.set(key, compact(render(mergedJson)))
    })
    pipeline.sync()
  }

}
