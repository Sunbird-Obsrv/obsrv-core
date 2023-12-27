package org.sunbird.obsrv.preprocessor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache._
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.Models._
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming._
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction

import scala.collection.mutable

class DeduplicationFunction(config: PipelinePreprocessorConfig)(implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseDatasetProcessFunction(config) with BaseDeduplication {

  private[this] val logger = LoggerFactory.getLogger(classOf[DeduplicationFunction])
  @transient private var dedupEngine: DedupEngine = null

  override def getMetrics(): List[String] = {
    List(config.duplicationTotalMetricsCount, config.duplicationSkippedEventMetricsCount, config.duplicationEventMetricsCount, config.duplicationProcessedEventMetricsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config.redisConnectionTimeout)
    dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)
  }

  override def close(): Unit = {
    super.close()
    dedupEngine.closeConnectionPool()
  }

  override def processElement(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(dataset.id, config.duplicationTotalMetricsCount)
    val dedupConfig = dataset.dedupConfig
    if (dedupConfig.isDefined && dedupConfig.get.dropDuplicates.get) {
      val event = msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]]
      val eventAsText = JSONUtil.serialize(event)
      val isDup = isDuplicate(dataset, dedupConfig.get.dedupKey, eventAsText, context)
      if (isDup) {
        metrics.incCounter(dataset.id, config.duplicationEventMetricsCount)
        context.output(config.duplicateEventsOutputTag, markFailed(msg, ErrorConstants.DUPLICATE_EVENT_FOUND, Producer.dedup))
      } else {
        metrics.incCounter(dataset.id, config.duplicationProcessedEventMetricsCount)
        context.output(config.uniqueEventsOutputTag, markSuccess(msg, Producer.dedup))
      }
    } else {
      metrics.incCounter(dataset.id, config.duplicationSkippedEventMetricsCount)
      context.output(config.uniqueEventsOutputTag, msg)
    }
  }

  private def isDuplicate(dataset: Dataset, dedupKey: Option[String], event: String,
                          context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context): Boolean = {
    try {
      super.isDuplicate(dataset.id, dedupKey, event)(dedupEngine)
    } catch {
      case ex: ObsrvException =>
        val sysEvent = JSONUtil.serialize(SystemEvent(
          EventID.METRIC,
          ctx = ContextData(module = ModuleID.processing, pdata = PData(config.jobName, PDataType.flink, Some(Producer.dedup)), dataset = Some(dataset.id), dataset_type = Some(dataset.datasetType)),
          data = EData(error = Some(ErrorLog(pdata_id = Producer.dedup, pdata_status = StatusCode.skipped, error_type = FunctionalError.DedupFailed, error_code = ex.error.errorCode, error_message = ex.error.errorMsg, error_level = ErrorLevel.warn)))
        ))
        logger.warn("BaseDeduplication:isDuplicate() | Exception", ex)
        context.output(config.systemEventsOutputTag, sysEvent)
        false
    }
  }

}
