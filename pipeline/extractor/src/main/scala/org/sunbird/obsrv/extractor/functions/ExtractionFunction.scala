package org.sunbird.obsrv.extractor.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.model.FunctionalError.FunctionalError
import org.sunbird.obsrv.core.model.Models._
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming.{BaseDeduplication, BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.Util.getMutableMap
import org.sunbird.obsrv.core.util.{JSONUtil, Util}
import org.sunbird.obsrv.extractor.task.ExtractorConfig
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

class ExtractionFunction(config: ExtractorConfig)
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) with BaseDeduplication {

  @transient private var dedupEngine: DedupEngine = null
  private[this] val logger = LoggerFactory.getLogger(classOf[ExtractionFunction])

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.successEventCount, config.systemEventCount, config.eventFailedMetricsCount, config.failedExtractionCount,
      config.skippedExtractionCount, config.duplicateExtractionCount, config.totalEventCount, config.successExtractionCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config.redisConnectionTimeout)
    dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpiryInSeconds)
  }

  override def processElement(batchEvent: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.defaultDatasetID, config.totalEventCount)
    if (batchEvent.contains(Constants.INVALID_JSON)) {
      context.output(config.failedBatchEventOutputTag, markBatchFailed(batchEvent, ErrorConstants.ERR_INVALID_EVENT))
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      context.output(config.systemEventsOutputTag, failedSystemEvent(Some(config.defaultDatasetID), ErrorConstants.ERR_INVALID_EVENT, FunctionalError.InvalidJsonData))
      return
    }
    val eventAsText = JSONUtil.serialize(batchEvent)
    val datasetIdOpt = batchEvent.get(config.CONST_DATASET)
    if (datasetIdOpt.isEmpty) {
      context.output(config.failedBatchEventOutputTag, markBatchFailed(batchEvent, ErrorConstants.MISSING_DATASET_ID))
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      context.output(config.systemEventsOutputTag, failedSystemEvent(Some(config.defaultDatasetID), ErrorConstants.MISSING_DATASET_ID, FunctionalError.MissingDatasetId))
      return
    }
    val datasetId = datasetIdOpt.get.asInstanceOf[String]
    metrics.incCounter(datasetId, config.totalEventCount)
    val datasetOpt = DatasetRegistry.getDataset(datasetId)
    if (datasetOpt.isEmpty) {
      context.output(config.failedBatchEventOutputTag, markBatchFailed(batchEvent, ErrorConstants.MISSING_DATASET_CONFIGURATION))
      metrics.incCounter(datasetId, config.failedExtractionCount)
      context.output(config.systemEventsOutputTag, failedSystemEvent(Some(datasetId), ErrorConstants.MISSING_DATASET_CONFIGURATION, FunctionalError.MissingDatasetId))
      return
    }
    val dataset = datasetOpt.get
    if (!containsEvent(batchEvent) && dataset.extractionConfig.isDefined && dataset.extractionConfig.get.isBatchEvent.get) {
      if (dataset.extractionConfig.get.dedupConfig.isDefined && dataset.extractionConfig.get.dedupConfig.get.dropDuplicates.get) {
        val isDup = isDuplicate(dataset, dataset.extractionConfig.get.dedupConfig.get.dedupKey, eventAsText, context)
        if (isDup) {
          metrics.incCounter(dataset.id, config.duplicateExtractionCount)
          context.output(config.duplicateEventOutputTag, markBatchFailed(batchEvent, ErrorConstants.DUPLICATE_BATCH_EVENT_FOUND))
          return
        }
      }
      extractData(dataset, batchEvent, eventAsText, context, metrics)
    } else {
      skipExtraction(dataset, batchEvent, context, metrics)
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
          ctx = ContextData(module = ModuleID.processing, pdata = PData(config.jobName, PDataType.flink, Some(Producer.extractor)), dataset = Some(dataset.id), dataset_type = Some(dataset.datasetType)),
          data = EData(error = Some(ErrorLog(pdata_id = Producer.dedup, pdata_status = StatusCode.skipped, error_type = FunctionalError.DedupFailed, error_code = ex.error.errorCode, error_message = ex.error.errorMsg, error_level = ErrorLevel.warn)))
        ))
        logger.warn("BaseDeduplication:isDuplicate() | Exception", ex)
        context.output(config.systemEventsOutputTag, sysEvent)
        false
    }
  }

  private def skipExtraction(dataset: Dataset, batchEvent: mutable.Map[String, AnyRef], context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                             metrics: Metrics): Unit = {
    val obsrvMeta = batchEvent(config.CONST_OBSRV_META).asInstanceOf[Map[String, AnyRef]]
    if (!super.containsEvent(batchEvent)) {
      metrics.incCounter(dataset.id, config.eventFailedMetricsCount)
      context.output(config.failedEventsOutputTag(), markBatchFailed(batchEvent, ErrorConstants.EVENT_MISSING))
      context.output(config.systemEventsOutputTag, failedSystemEvent(Some(dataset.id), ErrorConstants.EVENT_MISSING, FunctionalError.MissingEventData, dataset_type = Some(dataset.datasetType)))
      return
    }
    val eventData = Util.getMutableMap(batchEvent(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]])
    val eventJson = JSONUtil.serialize(eventData)
    val eventSize = eventJson.getBytes("UTF-8").length
    if (eventSize > config.eventMaxSize) {
      metrics.incCounter(dataset.id, config.eventFailedMetricsCount)
      context.output(config.failedEventsOutputTag(), markEventFailed(dataset.id, eventData, ErrorConstants.EVENT_SIZE_EXCEEDED, obsrvMeta))
      context.output(config.systemEventsOutputTag, failedSystemEvent(Some(dataset.id), ErrorConstants.EVENT_SIZE_EXCEEDED, FunctionalError.EventSizeExceeded, dataset_type = Some(dataset.datasetType)))
      logger.error(s"Extractor | Event size exceeded max configured value | dataset=${dataset.id} | Event size is $eventSize, Max configured size is ${config.eventMaxSize}")
    } else {
      metrics.incCounter(dataset.id, config.skippedExtractionCount)
      context.output(config.rawEventsOutputTag, markEventSkipped(dataset.id, eventData, obsrvMeta))
    }
  }

  private def extractData(dataset: Dataset, batchEvent: mutable.Map[String, AnyRef], eventAsText: String, context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                          metrics: Metrics): Unit = {
    try {
      val eventsList = getEventsList(dataset, eventAsText)
      val obsrvMeta = batchEvent(config.CONST_OBSRV_META).asInstanceOf[Map[String, AnyRef]]
      eventsList.foreach(immutableEvent => {
        val eventData = getMutableMap(immutableEvent)
        val eventJson = JSONUtil.serialize(eventData)
        val eventSize = eventJson.getBytes("UTF-8").length
        if (eventSize > config.eventMaxSize) {
          metrics.incCounter(dataset.id, config.eventFailedMetricsCount)
          context.output(config.failedEventsOutputTag(), markEventFailed(dataset.id, eventData, ErrorConstants.EVENT_SIZE_EXCEEDED, obsrvMeta))
          context.output(config.systemEventsOutputTag, failedSystemEvent(Some(dataset.id), ErrorConstants.EVENT_SIZE_EXCEEDED, FunctionalError.EventSizeExceeded, dataset_type = Some(dataset.datasetType)))
          logger.error(s"Extractor | Event size exceeded max configured value | dataset=${dataset.id} | Event size is $eventSize, Max configured size is ${config.eventMaxSize}")
        } else {
          metrics.incCounter(dataset.id, config.successEventCount)
          context.output(config.rawEventsOutputTag, markEventSuccess(dataset.id, eventData, obsrvMeta))
        }
      })
      context.output(config.systemEventsOutputTag, JSONUtil.serialize(successSystemEvent(dataset, eventsList.size)))
      metrics.incCounter(dataset.id, config.systemEventCount)
      metrics.incCounter(dataset.id, config.successExtractionCount)
    } catch {
      case ex: ObsrvException =>
        metrics.incCounter(dataset.id, config.failedExtractionCount)
        context.output(config.failedBatchEventOutputTag, markBatchFailed(batchEvent, ex.error))
        context.output(config.systemEventsOutputTag, failedSystemEvent(Some(dataset.id), ex.error, FunctionalError.ExtractionDataFormatInvalid, dataset_type = Some(dataset.datasetType)))
        logger.error(s"Extractor | Exception extracting data | dataset=${dataset.id}", ex)
    }

  }

  private def getEventsList(dataset: Dataset, eventAsText: String): List[Map[String, AnyRef]] = {
    val node = JSONUtil.getKey(dataset.extractionConfig.get.extractionKey.get, eventAsText)
    if (node.isMissingNode) {
      throw new ObsrvException(ErrorConstants.NO_EXTRACTION_DATA_FOUND)
    } else if (!node.isArray) {
      throw new ObsrvException(ErrorConstants.EXTRACTED_DATA_NOT_A_LIST)
    } else {
      JSONUtil.deserialize[List[Map[String, AnyRef]]](node.toString); // TODO: Check if this is the better way to get JsonNode data
    }
  }

  private def updateEvent(event: mutable.Map[String, AnyRef], obsrvMeta: Map[String, AnyRef]) = {
    event.put(config.CONST_OBSRV_META, obsrvMeta)
  }

  /**
   * Method to Generate a System Event to capture the extraction information and metrics
   */
  private def successSystemEvent(dataset: Dataset, totalEvents: Int): SystemEvent = {
    SystemEvent(
      EventID.METRIC,
      ctx = ContextData(module = ModuleID.processing, pdata = PData(config.jobName, PDataType.flink, Some(Producer.extractor)), dataset = Some(dataset.id), dataset_type = Some(dataset.datasetType)),
      data = EData(error = None, pipeline_stats = Some(PipelineStats(Some(totalEvents), Some(StatusCode.success))))
    )
  }

  private def failedSystemEvent(dataset: Option[String], error: Error, functionalError: FunctionalError, dataset_type: Option[String] = None): String = {

    JSONUtil.serialize(SystemEvent(
      EventID.METRIC, ctx = ContextData(module = ModuleID.processing, pdata = PData(config.jobName, PDataType.flink, Some(Producer.extractor)), dataset = dataset, dataset_type = dataset_type),
      data = EData(error = Some(ErrorLog(Producer.extractor, StatusCode.failed, functionalError, error.errorCode, error.errorMsg, ErrorLevel.critical)), pipeline_stats = None)
    ))
  }

  /**
   * Method Mark the event as failure by adding (ex_processed -> false) and metadata.
   */
  private def markEventFailed(dataset: String, event: mutable.Map[String, AnyRef], error: Error, obsrvMeta: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val wrapperEvent = createWrapperEvent(dataset, event)
    updateEvent(wrapperEvent, obsrvMeta)
    super.markFailed(wrapperEvent, error, Producer.extractor)
    wrapperEvent
  }

  private def markBatchFailed(batchEvent: mutable.Map[String, AnyRef], error: Error): mutable.Map[String, AnyRef] = {
    super.markFailed(batchEvent, error, Producer.extractor)
    batchEvent
  }

  private def markEventSuccess(dataset: String, event: mutable.Map[String, AnyRef], obsrvMeta: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val wrapperEvent = createWrapperEvent(dataset, event)
    updateEvent(wrapperEvent, obsrvMeta)
    super.markSuccess(wrapperEvent, Producer.extractor)
    wrapperEvent
  }

  private def markEventSkipped(dataset: String, event: mutable.Map[String, AnyRef], obsrvMeta: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val wrapperEvent = createWrapperEvent(dataset, event)
    updateEvent(wrapperEvent, obsrvMeta)
    super.markSkipped(wrapperEvent, Producer.extractor)
    wrapperEvent
  }

  private def createWrapperEvent(dataset: String, event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    mutable.Map(config.CONST_DATASET -> dataset, config.CONST_EVENT -> event.toMap)
  }
}