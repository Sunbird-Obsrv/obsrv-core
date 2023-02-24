package org.sunbird.obsrv.preprocessor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache._
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.streaming._
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

class DeduplicationFunction(config: PipelinePreprocessorConfig, @transient var dedupEngine: DedupEngine = null)
                           (implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DeduplicationFunction])

  override def getMetricsList(): MetricsList = {
    val metrics = List(
      config.duplicationTotalMetricsCount, config.duplicationSkippedEventMetricsCount, config.duplicationEventMetricsCount,
      config.duplicationProcessedEventMetricsCount, config.eventFailedMetricsCount
    )
    MetricsList(DatasetRegistry.getDataSetIds(), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (dedupEngine == null) {
      val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config.redisConnectionTimeout)
      dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)
    }
  }

  override def close(): Unit = {
    super.close()
    dedupEngine.closeConnectionPool()
  }

  override def processElement(msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(config.defaultDatasetID, config.duplicationTotalMetricsCount)
    val datasetId = msg.get(config.CONST_DATASET)
    if (datasetId.isEmpty) {
      context.output(config.failedEventsOutputTag, markFailed(msg, ErrorConstants.MISSING_DATASET_ID, config.jobName))
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return
    }
    val datasetOpt = DatasetRegistry.getDataset(datasetId.get.asInstanceOf[String])
    if (datasetOpt.isEmpty) {
      context.output(config.failedEventsOutputTag, markFailed(msg, ErrorConstants.MISSING_DATASET_CONFIGURATION, "Deduplication"))
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return
    }
    val dataset = datasetOpt.get
    val dedupConfig = dataset.dedupConfig
    if (dedupConfig.isDefined && dedupConfig.get.dropDuplicates.get) {
      val event = msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]]
      val eventAsText = JSONUtil.serialize(event)
      val isDup = isDuplicate(dedupConfig.get.dedupKey, eventAsText, context, config)(dedupEngine)
      if (isDup) {
        metrics.incCounter(dataset.id, config.duplicationEventMetricsCount)
        context.output(config.duplicateEventsOutputTag, markFailed(msg, ErrorConstants.DUPLICATE_EVENT_FOUND, "Deduplication"))
      } else {
        metrics.incCounter(dataset.id, config.duplicationProcessedEventMetricsCount)
        context.output(config.uniqueEventsOutputTag, markSuccess(msg, "Deduplication"))
      }
    } else {
      metrics.incCounter(dataset.id, config.duplicationSkippedEventMetricsCount)
      context.output(config.uniqueEventsOutputTag, msg)
    }
  }

}
