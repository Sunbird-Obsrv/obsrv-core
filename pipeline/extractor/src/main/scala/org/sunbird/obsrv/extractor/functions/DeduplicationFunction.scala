package org.sunbird.obsrv.extractor.functions

import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.extractor.task.TelemetryExtractorConfig

import java.util

// import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisException

class DeduplicationFunction(config: TelemetryExtractorConfig, @transient var dedupEngine: DedupEngine = null)
                           (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[String, util.Map[String, AnyRef]](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DeduplicationFunction])

  override def metricsList(): List[String] = {
    List(config.successBatchCount, config.failedBatchCount) ::: deduplicationMetrics
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (dedupEngine == null) {
      val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config)
      dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)
    }
  }

  override def close(): Unit = {
    super.close()
    dedupEngine.closeConnectionPool()
  }

  override def processElement(batchEvents: String,
                              context: ProcessFunction[String, util.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
    try {
      deDup[String, util.Map[String, AnyRef]](getMsgIdentifier(batchEvents),
        batchEvents,
        context,
        config.uniqueEventOutputTag,
        config.duplicateEventOutputTag,
        flagName = "extractor_duplicate")(dedupEngine, metrics)
      metrics.incCounter(config.successBatchCount)
    } catch {
      case jedisEx: JedisException => {
        logger.info("Exception when retrieving data from redis " + jedisEx.getMessage)
        dedupEngine.getRedisConnection.close()
        throw jedisEx
      }
      case ex: Exception => {
        logger.error("Unexpected Error", ex)
        metrics.incCounter(config.failedBatchCount)
        context.output(config.failedBatchEventOutputTag, batchEvents)
      }
    }

    def getMsgIdentifier(batchEvents: String): String = {
      // val event = new Gson().fromJson(batchEvents, new util.LinkedHashMap[String, AnyRef]().getClass)
      val event = JSONUtil.deserialize[util.LinkedHashMap[String, AnyRef]](batchEvents)
      val paramsObj = Option(event.get("params"))
      val messageId = paramsObj.map {
        params => params.asInstanceOf[util.Map[String, AnyRef]].get("msgid").asInstanceOf[String]
      }
      messageId.orNull
    }
  }
}
