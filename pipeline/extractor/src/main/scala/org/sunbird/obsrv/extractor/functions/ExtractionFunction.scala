package org.sunbird.obsrv.extractor.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.model.Models.{PData, SystemEvent}
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.Util.getMutableMap
import org.sunbird.obsrv.core.util.{JSONUtil, Util}
import org.sunbird.obsrv.extractor.task.ExtractorConfig
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry
import org.slf4j.LoggerFactory

import scala.collection.mutable

class ExtractionFunction(config: ExtractorConfig, @transient var dedupEngine: DedupEngine = null)
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ExtractionFunction])

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.successEventCount, config.systemEventCount, config.failedEventCount, config.failedExtractionCount,
      config.skippedExtractionCount, config.duplicateExtractionCount, config.totalEventCount, config.successExtractionCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (dedupEngine == null) {
      val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config.redisConnectionTimeout)
      dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpiryInSeconds)
    }
  }

  override def processElement(batchEvent: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
    metrics.incCounter(config.defaultDatasetID, config.totalEventCount)
    if(batchEvent.contains("invalidEvent")) {
      batchEvent.remove("invalidEvent")
      context.output(config.failedEventsOutputTag, markBatchFailed(batchEvent, ErrorConstants.ERE_INVALID_EVENT, ""))
      metrics.incCounter(config.defaultDatasetID, config.failedEventCount)
      return
    }
    val datasetId = batchEvent.get(config.CONST_DATASET)
    if (datasetId.isEmpty) {
      context.output(config.failedBatchEventOutputTag, markBatchFailed(batchEvent, ErrorConstants.MISSING_DATASET_ID, ""))
      metrics.incCounter(config.defaultDatasetID, config.failedExtractionCount)
      return
    }
    val datasetOpt = DatasetRegistry.getDataset(datasetId.get.asInstanceOf[String])
    if (datasetOpt.isEmpty) {
      context.output(config.failedBatchEventOutputTag, markBatchFailed(batchEvent, ErrorConstants.MISSING_DATASET_CONFIGURATION, ""))
      metrics.incCounter(config.defaultDatasetID, config.failedExtractionCount)
      return
    }
    val dataset = datasetOpt.get
    validateExtractionConfig(dataset, JSONUtil.serialize(batchEvent))
    val extractionKey = dataset.extractionConfig.get.extractionKey.get
    if (!containsEvent(batchEvent) && dataset.extractionConfig.isDefined && dataset.extractionConfig.get.isBatchEvent.get) {
      val eventAsText = JSONUtil.serialize(batchEvent)
      if (dataset.extractionConfig.get.dedupConfig.isDefined && dataset.extractionConfig.get.dedupConfig.get.dropDuplicates.get) {
        val isDup = isDuplicate(dataset.id, dataset.extractionConfig.get.dedupConfig.get.dedupKey, eventAsText, context, config)(dedupEngine)
        if (isDup) {
          metrics.incCounter(dataset.id, config.duplicateExtractionCount)
          context.output(config.duplicateEventOutputTag, markBatchFailed(batchEvent, ErrorConstants.DUPLICATE_BATCH_EVENT_FOUND, extractionKey))
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
    val obsrvMeta = batchEvent(config.CONST_OBSRV_META).asInstanceOf[Map[String, AnyRef]]
    if (!super.containsEvent(batchEvent)) {
      metrics.incCounter(dataset.id, config.failedEventCount)
      context.output(config.failedEventsOutputTag, markBatchFailed(batchEvent, ErrorConstants.EVENT_MISSING, dataset.extractionConfig.get.extractionKey.get))
      return
    }
    val eventData = Util.getMutableMap(batchEvent(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]])
    val eventJson = JSONUtil.serialize(eventData)
    val eventSize = eventJson.getBytes("UTF-8").length
    if (eventSize > config.eventMaxSize) {
      metrics.incCounter(dataset.id, config.failedEventCount)
      context.output(config.failedEventsOutputTag, markEventFailed(dataset.id, eventData, ErrorConstants.EVENT_SIZE_EXCEEDED, obsrvMeta))
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
          metrics.incCounter(dataset.id, config.failedEventCount)
          context.output(config.failedEventsOutputTag, markEventFailed(dataset.id, eventData, ErrorConstants.EVENT_SIZE_EXCEEDED, obsrvMeta))
        } else {
          metrics.incCounter(dataset.id, config.successEventCount)
          context.output(config.rawEventsOutputTag, markEventSuccess(dataset.id, eventData, obsrvMeta))
        }
      })
      context.output(config.systemEventsOutputTag, JSONUtil.serialize(generateSystemEvent(dataset.id, eventsList.size)))
      metrics.incCounter(dataset.id, config.systemEventCount)
      metrics.incCounter(dataset.id, config.successExtractionCount)
    } catch {
      case ex: ObsrvException =>
        context.output(config.failedBatchEventOutputTag, markBatchFailed(batchEvent, ex.error, dataset.extractionConfig.get.extractionKey.get))
        metrics.incCounter(dataset.id, config.failedExtractionCount)
      case re: Exception => re.printStackTrace()
    }

  }

  private def getEventsList(dataset: Dataset, eventAsText: String): List[Map[String, AnyRef]] = {
    val node = JSONUtil.getKey(dataset.extractionConfig.get.extractionKey.get, eventAsText)
    JSONUtil.deserialize[List[Map[String, AnyRef]]](node.toString); // TODO: Check if this is the better way to get JsonNode data
  }

  private def validateExtractionConfig(dataset: Dataset, eventAsText: String): Unit = {
    val node = JSONUtil.getKey(dataset.extractionConfig.get.extractionKey.get, eventAsText)
    if (node.isMissingNode) {
      throw new ObsrvException(ErrorConstants.NO_EXTRACTION_DATA_FOUND)
    } else if (!node.isArray) {
      throw new ObsrvException(ErrorConstants.EXTRACTED_DATA_NOT_A_LIST)
    }
  }

  private def updateEvent(event: mutable.Map[String, AnyRef], obsrvMeta: Map[String, AnyRef]) = {
    event.put(config.CONST_OBSRV_META, obsrvMeta)
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
  private def markEventFailed(dataset: String, event: mutable.Map[String, AnyRef], error: Error, obsrvMeta: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val wrapperEvent = createWrapperEvent(dataset, event)
    updateEvent(wrapperEvent, obsrvMeta)
    super.markFailed(wrapperEvent, error, config.jobName)
    wrapperEvent
  }

  private def markBatchFailed(batchEvent: mutable.Map[String, AnyRef], error: Error, extractionKey: String): mutable.Map[String, AnyRef] = {
    if (!extractionKey.isEmpty) {
      batchEvent.put("event", batchEvent.get(extractionKey))
      batchEvent.remove(extractionKey)
    }
    super.markFailed(batchEvent, error, config.jobName)
    batchEvent
  }

  private def markEventSuccess(dataset: String, event: mutable.Map[String, AnyRef], obsrvMeta: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val wrapperEvent = createWrapperEvent(dataset, event)
    updateEvent(wrapperEvent, obsrvMeta)
    super.markSuccess(wrapperEvent, config.jobName)
    wrapperEvent
  }

  private def markEventSkipped(dataset: String, event: mutable.Map[String, AnyRef], obsrvMeta: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val wrapperEvent = createWrapperEvent(dataset, event)
    updateEvent(wrapperEvent, obsrvMeta)
    super.markSkipped(wrapperEvent, config.jobName)
    wrapperEvent
  }


  private def createWrapperEvent(dataset: String, event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    mutable.Map(config.CONST_DATASET -> dataset, config.CONST_EVENT -> event.toMap)
  }
}

