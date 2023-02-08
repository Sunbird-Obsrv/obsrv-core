package org.sunbird.obsrv.extractor.functions

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.model.Models.{PData, SystemEvent}
import org.sunbird.obsrv.core.streaming.{BaseDeduplication, BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.extractor.task.ExtractorConfig
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

class ExtractionFunction(config: ExtractorConfig, @transient var dedupEngine: DedupEngine = null)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) with BaseDeduplication {


  override def getMetricsList(): MetricsList = {
    val metrics = List(config.successEventCount, config.systemEventCount, config.failedEventCount, config.failedExtractionCount,
      config.skippedExtractionCount, config.duplicateExtractionCount, config.totalEventCount, config.successExtractionCount)
    MetricsList(DatasetRegistry.getDataSetIds(), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (dedupEngine == null) {
      val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config)
      dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpiryInSeconds)
    }
  }

  override def processElement(batchEvent: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.defaultDatasetID, config.totalEventCount)
    val datasetId = batchEvent.get("dataset")
    if (datasetId.isEmpty) {
      markBatchFailed(batchEvent, ErrorConstants.MISSING_DATASET_ID)
      context.output(config.failedBatchEventOutputTag, batchEvent)
      metrics.incCounter(config.defaultDatasetID, config.failedExtractionCount)
      return
    }
    val datasetOpt = DatasetRegistry.getDataset(datasetId.get.asInstanceOf[String])
    if (datasetOpt.isEmpty) {
      markBatchFailed(batchEvent, ErrorConstants.MISSING_DATASET_CONFIGURATION)
      context.output(config.failedBatchEventOutputTag, batchEvent)
      metrics.incCounter(config.defaultDatasetID, config.failedExtractionCount)
      return
    }
    val dataset = datasetOpt.get
    val eventAsText = JSONUtil.serialize(batchEvent)
    if (dataset.extractionConfig.isDefined && dataset.extractionConfig.get.isBatchEvent.get) {
      if (dataset.extractionConfig.get.dedupConfig.isDefined && dataset.extractionConfig.get.dedupConfig.get.dropDuplicates.get) {
        val isDup = isDuplicate(dataset.extractionConfig.get.dedupConfig.get.dedupKey, eventAsText, context, config)(dedupEngine)
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

  private def skipExtraction(dataset: Dataset, batchEvent: mutable.Map[String, AnyRef], context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                             metrics: Metrics): Unit = {
    val syncTs = batchEvent.get("syncts").getOrElse(System.currentTimeMillis()).asInstanceOf[Number].longValue()
    val immutableEvent = batchEvent.get("event")
    if(immutableEvent.isEmpty || !immutableEvent.get.isInstanceOf[Map[String, AnyRef]]) {
      metrics.incCounter(dataset.id, config.failedEventCount)
      context.output(config.failedEventsOutputTag, markFailed(dataset.id, batchEvent, ErrorConstants.EVENT_MISSING))
      return
    }
    val eventData = getMutableMap(immutableEvent.get.asInstanceOf[Map[String, AnyRef]])
    updateEvent(eventData, syncTs)
    val eventJson = JSONUtil.serialize(eventData)
    val eventSize = eventJson.getBytes("UTF-8").length
    if (eventSize > config.eventMaxSize) {
      metrics.incCounter(dataset.id, config.failedEventCount)
      context.output(config.failedEventsOutputTag, markFailed(dataset.id, eventData, ErrorConstants.EVENT_SIZE_EXCEEDED))
    } else {
      metrics.incCounter(dataset.id, config.skippedExtractionCount)
      context.output(config.rawEventsOutputTag, markSkipped(dataset.id, eventData))
    }
  }

  private def extractData(dataset: Dataset, batchEvent: mutable.Map[String, AnyRef], eventAsText: String, context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                          metrics: Metrics): Unit = {
    try {
      val eventsList = getEventsList(dataset, eventAsText)
      val syncTs = batchEvent.get("syncts").getOrElse(System.currentTimeMillis()).asInstanceOf[Number].longValue()
      eventsList.foreach(immutableEvent => {
        val eventData = getMutableMap(immutableEvent)
        updateEvent(eventData, syncTs)
        val eventJson = JSONUtil.serialize(eventData)
        val eventSize = eventJson.getBytes("UTF-8").length
        if (eventSize > config.eventMaxSize) {
          metrics.incCounter(dataset.id, config.failedEventCount)
          context.output(config.failedEventsOutputTag, markFailed(dataset.id, eventData, ErrorConstants.EVENT_SIZE_EXCEEDED))
        } else {
          metrics.incCounter(dataset.id, config.successEventCount)
          context.output(config.rawEventsOutputTag, markSuccess(dataset.id, eventData))
        }
      })
      context.output(config.systemEventsOutputTag, JSONUtil.serialize(generateSystemEvent(dataset.id, eventsList.size)))
      metrics.incCounter(dataset.id, config.systemEventCount)
      metrics.incCounter(dataset.id, config.successExtractionCount)
    } catch {
      case ex: ObsrvException =>
        markBatchFailed(batchEvent, ex.error)
        context.output(config.failedBatchEventOutputTag, batchEvent)
        metrics.incCounter(dataset.id, config.failedExtractionCount)
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

  private def updateEvent(event: mutable.Map[String, AnyRef], syncts: Long) = {
    event.put("syncts", syncts.asInstanceOf[AnyRef])
  }

  /**
   * Method to Generate a System Event to capture the extraction information and metrics
   */
  private def generateSystemEvent(dataset: String, totalEvents: Int): SystemEvent = {
    SystemEvent(PData(config.jobName, "flink", ""), Map("totalEvents" -> totalEvents.asInstanceOf[AnyRef], "dataset" -> dataset.asInstanceOf[AnyRef])); // TODO: Generate a system event
  }

  /**
   * Method Mark the event as failure by adding (ex_processed -> false) and metadata.
   */
  private def markFailed(dataset: String, event: mutable.Map[String, AnyRef], error: Error): mutable.Map[String, AnyRef] = {
    addError(event, Map("src" -> config.jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    addFlags(event, Map("extraction_processed" -> "no"))
    createWrapperEvent(dataset, event)
  }

  private def markBatchFailed(batchEvent: mutable.Map[String, AnyRef], error: Error): mutable.Map[String, AnyRef] = {
    addFlags(batchEvent, Map("extraction_processed" -> "no"))
    addError(batchEvent, Map("src" -> config.jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    batchEvent
  }

  private def markSuccess(dataset: String, event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val wrapperEvent = createWrapperEvent(dataset, event)
    addFlags(wrapperEvent, Map("extraction_processed" -> "yes"))
    wrapperEvent
  }

  private def markSkipped(dataset: String, event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val wrapperEvent = createWrapperEvent(dataset, event)
    addFlags(wrapperEvent, Map("extraction_processed" -> "skipped"))
    wrapperEvent
  }


  private def createWrapperEvent(dataset: String, event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    mutable.Map("dataset" -> dataset, "event" -> event)
  }
}

