package org.sunbird.obsrv.preprocessor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache._
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.streaming._
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

class DeduplicationFunction(config: PipelinePreprocessorConfig,
                            @transient var dedupEngine: DedupEngine = null)
                           (implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) with BaseDeduplication {

  private[this] val logger = LoggerFactory.getLogger(classOf[DeduplicationFunction])

  override def getMetricsList(): MetricsList = {
    val metrics = List(
      config.duplicationSkippedEventMetricsCount,
      config.duplicationEventMetricsCount,
      config.duplicationProcessedEventMetricsCount,
      config.eventFailedMetricsCount
    )
    MetricsList(DatasetRegistry.getDataSetIds(), metrics);
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

  override def processElement(msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    val datasetId = msg.get("dataset");
    if (datasetId.isEmpty) {
      markFailed(msg, ErrorConstants.MISSING_DATASET_ID);
      context.output(config.failedEventsOutputTag, msg)
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return;
    }
    val datasetOpt = DatasetRegistry.getDataset(datasetId.get.asInstanceOf[String])
    if (datasetOpt.isEmpty) {
      markFailed(msg, ErrorConstants.MISSING_DATASET_CONFIGURATION);
      context.output(config.failedEventsOutputTag, msg)
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return;
    }
    val dataset = datasetOpt.get;
    val dedupConfig = dataset.dedupConfig;
    val event = msg.get("event").get.asInstanceOf[Map[String, AnyRef]];
    val eventAsText = JSONUtil.serialize(event);
    if (dedupConfig.isDefined && dedupConfig.get.dropDuplicates.get) {
      val isDup = isDuplicate(dataset.extractionConfig.get.dedupConfig.get.dedupKey, eventAsText, context, config)(dedupEngine)
      if (isDup) {
        metrics.incCounter(dataset.id, config.duplicationEventMetricsCount);
        context.output(config.duplicateEventsOutputTag, markFailed(msg, ErrorConstants.DUPLICATE_EVENT_FOUND))
        return
      }
    } else {
      metrics.incCounter(dataset.id, config.duplicationSkippedEventMetricsCount)
      context.output(config.uniqueEventsOutputTag, msg)
    }
  }

  private def markFailed(batchEvent: mutable.Map[String, AnyRef], error: Error): mutable.Map[String, AnyRef] = {
    addFlags(batchEvent, Map("proprocessing_processed" -> "no"))
    addError(batchEvent, Map("src" -> config.jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    batchEvent
  }

}
