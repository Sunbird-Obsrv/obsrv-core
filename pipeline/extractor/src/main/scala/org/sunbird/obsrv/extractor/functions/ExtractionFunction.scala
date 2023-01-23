package org.sunbird.obsrv.extractor.functions

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
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

import java.lang.reflect.Type
import scala.collection.mutable

class ExtractionFunction(config: ExtractorConfig, @transient var dedupEngine: DedupEngine = null)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) with BaseDeduplication {

  val mapType: Type = new TypeToken[mutable.Map[String, AnyRef]]() {}.getType

  val gson = new Gson()

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.successEventCount, config.systemEventCount, config.failedEventCount, config.skippedExtractionCount, config.duplicateExtractionCount)
    MetricsList(DatasetRegistry.getDataSetIds(), metrics);
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (dedupEngine == null) {
      val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config)
      dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpiryInSeconds)
    }
  }

  /**
   * Method to process the events extraction from the batch
   *
   * @param batchEvent - Batch of extractable events
   * @param context
   */
  override def processElement(batchEvent: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    val datasetId = batchEvent.get("dataset");
    if(datasetId.isEmpty) {
      markBatchFailed(batchEvent, ErrorConstants.MISSING_DATASET_ID);
      context.output(config.failedBatchEventOutputTag, batchEvent)
      metrics.incCounter(config.defaultDatasetID, config.failedExtractionCount)
      return;
    }
    val datasetOpt = DatasetRegistry.getDataset(datasetId.get.asInstanceOf[String])
    if(datasetOpt.isEmpty) {
      markBatchFailed(batchEvent, ErrorConstants.MISSING_DATASET_CONFIGURATION);
      context.output(config.failedBatchEventOutputTag, batchEvent)
      metrics.incCounter(config.defaultDatasetID, config.failedExtractionCount)
      return;
    }
    val dataset = datasetOpt.get;
    val eventAsText = JSONUtil.serialize(batchEvent)
    if(dataset.extractionConfig.isDefined && dataset.extractionConfig.get.isBatchEvent.get) {
      if(dataset.extractionConfig.get.dedupConfig.isDefined && dataset.extractionConfig.get.dedupConfig.get.dropDuplicates.get) {
        val isDup = isDuplicate(dataset.extractionConfig.get.dedupConfig.get.dedupKey, eventAsText, context, config)(dedupEngine)
        if(isDup) {
          metrics.incCounter(dataset.id, config.duplicateExtractionCount);
          context.output(config.duplicateEventOutputTag, markBatchFailed(batchEvent, ErrorConstants.DUPLICATE_BATCH_EVENT_FOUND))
          return
        }
      }
      extractData(dataset, batchEvent, eventAsText, context, metrics);
    } else {
      context.output(config.rawEventsOutputTag, markSkipped(dataset.id, batchEvent))
      metrics.incCounter(dataset.id, config.skippedExtractionCount)
    }
  }

  def extractData(dataset: Dataset, batchEvent: mutable.Map[String, AnyRef], eventAsText: String, context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                  metrics: Metrics): Unit = {
    try {
      val eventsList = getEventsList(dataset, eventAsText);
      val syncTs = Option(batchEvent.get("syncts")).getOrElse(System.currentTimeMillis()).asInstanceOf[Number].longValue()
      eventsList.map(immutableEvent => {
        val eventData = getMutableMap(immutableEvent);
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
      context.output(config.systemEventsOutputTag, gson.toJson(generateSystemEvent(dataset.id, eventsList.size, batchEvent)))
      metrics.incCounter(dataset.id, config.systemEventCount)
      metrics.incCounter(dataset.id, config.successExtractionCount);
    } catch {
      case ex: ObsrvException =>
        markBatchFailed(batchEvent, ex.error);
        context.output(config.failedBatchEventOutputTag, batchEvent)
        metrics.incCounter(dataset.id, config.failedExtractionCount)
    }

  }

  /**
   * Method to get the events from the batch.
   *
   * @param batchEvent - Batch of telemetry event.
   * @return Array[AnyRef] - List of telemetry events.
   */
  def getEventsList(dataset:Dataset, eventAsText: String): List[Map[String, AnyRef]] = {
    val node = JSONUtil.getKey(dataset.extractionConfig.get.extractionKey.get, eventAsText);
    if(node.isMissingNode) {
      throw new ObsrvException(ErrorConstants.NO_EXTRACTION_DATA_FOUND)
    } else if(!node.isArray) {
      throw new ObsrvException(ErrorConstants.EXTRACTED_DATA_NOT_A_LIST)
    } else {
      JSONUtil.deserialize[List[Map[String, AnyRef]]](node.toString); // TODO: Check if this is the better way to get JsonNode data
    }
  }

  /**
   * Method to update the "SyncTS", "@TimeStamp" fileds of batch events into Events Object
   *
   * @param event  - Extracted Raw Telemetry Event
   * @param syncts - sync timestamp epoch to be updated in the events
   * @return - util.Map[String, AnyRef] Updated Telemetry Event
   */
  def updateEvent(event: mutable.Map[String, AnyRef], syncts: Long) = {
    event.put("syncts", syncts.asInstanceOf[AnyRef])
  }

  /**
   * Method to Generate a System Event to capture the extraction information and metrics
   */
  def generateSystemEvent(dataset: String, totalEvents: Int, batchEvent: mutable.Map[String, AnyRef]): SystemEvent = {
    SystemEvent(PData(config.jobName, "flink", ""), Map("totalEvents" -> totalEvents.asInstanceOf[AnyRef], "dataset" -> dataset.asInstanceOf[AnyRef])); // TODO: Generate a system event
  }

  /**
   * Method Mark the event as failure by adding (ex_processed -> false) and metadata.
   */
  def markFailed(dataset: String, event: mutable.Map[String, AnyRef], error: Error):mutable.Map[String, AnyRef] = {
    addError(event, Map("src" -> config.jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    addFlags(event, Map("extraction_processed" -> "no"));
    createWrapperEvent(dataset, event);
  }

  def markBatchFailed(batchEvent: mutable.Map[String, AnyRef], error: Error): mutable.Map[String, AnyRef] = {
    addFlags(batchEvent, Map("extraction_processed" -> "no"))
    addError(batchEvent, Map("src" -> config.jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    batchEvent
  }

  /**
   * Method to mark the event as success by adding flags adding (ex_processed -> true)
   *
   * @param event
   * @return
   */
  def markSuccess(dataset:String, event: mutable.Map[String, AnyRef]):mutable.Map[String, AnyRef] = {
    addFlags(event, Map("extraction_processed" -> "yes"))
    createWrapperEvent(dataset, event);
  }

  def markSkipped(dataset: String, event: mutable.Map[String, AnyRef]):mutable.Map[String, AnyRef] = {
    addFlags(event, Map("extraction_processed" -> "skipped"))
    createWrapperEvent(dataset, event);
  }



  def createWrapperEvent(dataset: String, event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    mutable.Map("dataset" -> dataset, "event" -> event)
  }
}

