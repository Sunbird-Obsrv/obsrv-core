package org.sunbird.obsrv.denormalizer.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics}
import org.sunbird.obsrv.denormalizer.domain.Event
import org.sunbird.obsrv.denormalizer.task.DenormalizationConfig

class SummaryDeduplicationFunction(config: DenormalizationConfig, @transient var dedupEngine: DedupEngine = null)
                                  (implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  override def metricsList(): List[String] = {
    deduplicationMetrics
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

  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
      deDup[Event, Event](event.mid(), event, context, config.uniqueSummaryEventsOutputTag, config.duplicateEventsOutputTag, flagName = config.DEDUP_FLAG_NAME)(dedupEngine, metrics)
  }

}

