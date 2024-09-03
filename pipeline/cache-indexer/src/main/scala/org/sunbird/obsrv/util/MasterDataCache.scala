package org.sunbird.obsrv.util

import org.json4s.native.JsonMethods._
import org.json4s.{JField, JNothing, JValue}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.core.model.Constants.OBSRV_META
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.pipeline.task.CacheIndexerConfig
import redis.clients.jedis.Jedis

import scala.collection.mutable

class MasterDataCache(val config: CacheIndexerConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MasterDataCache])
  private val datasetPipelineMap: mutable.Map[String, Jedis] = mutable.Map[String, Jedis]()

  def close(): Unit = {
    datasetPipelineMap.values.foreach(pipeline => pipeline.close())
  }

  def open(datasets: List[Dataset]): Unit = {
    datasets.foreach(dataset => {
      open(dataset)
    })
  }

  def open(dataset: Dataset): Unit = {
    if (!datasetPipelineMap.contains(dataset.id)) {
      val redisConfig = dataset.datasetConfig.cacheConfig.get
      val redisConnect = new RedisConnect(redisConfig.redisDBHost.get, redisConfig.redisDBPort.get, config.redisConnectionTimeout)
      val jedis: Jedis = redisConnect.getConnection(0)
      datasetPipelineMap.put(dataset.id, jedis)
    }
  }

  def process(dataset: Dataset, key: String, event: JValue): (Int, Int) = {
    val jedis = this.datasetPipelineMap(dataset.id)
    val dataFromCache = getDataFromCache(dataset, key, jedis)
    val updatedEvent = event.removeField {
      case JField(OBSRV_META, _) => true
      case _ => false
    }
    updateCache(dataset, dataFromCache, key, updatedEvent, jedis)
    (if (dataFromCache == null) 1 else 0, if (dataFromCache == null) 0 else 1)
  }

  private def getDataFromCache(dataset: Dataset, key: String, jedis: Jedis): String = {

    jedis.select(dataset.datasetConfig.cacheConfig.get.redisDB.get)
    jedis.get(key)
  }

  private def updateCache(dataset: Dataset, dataFromCache: String, key: String, event: JValue, jedis: Jedis): Unit = {

    jedis.select(dataset.datasetConfig.cacheConfig.get.redisDB.get)
    val existingJson = if (dataFromCache != null) parse(dataFromCache) else JNothing
    val mergedJson = existingJson merge event
    jedis.set(key, compact(render(mergedJson)))
  }

}
