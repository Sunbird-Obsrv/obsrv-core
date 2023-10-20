package org.sunbird.obsrv.pipeline.function

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.{Metrics, MetricsList, WindowBaseProcessFunction}
import org.sunbird.obsrv.pipeline.task.MasterDataProcessorConfig
import org.sunbird.obsrv.pipeline.util.MasterDataCache
import org.sunbird.obsrv.registry.DatasetRegistry
import org.json4s._
import org.json4s.native.JsonMethods._
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.util.JSONUtil

import java.lang
import scala.collection.mutable
import scala.collection.JavaConverters._

class MasterDataProcessorFunction(config: MasterDataProcessorConfig) extends WindowBaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MasterDataProcessorFunction])
  private[this] var masterDataCache: MasterDataCache = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    masterDataCache = new MasterDataCache(config)
    masterDataCache.open(DatasetRegistry.getAllDatasets(config.datasetType()))
  }

  override def close(): Unit = {
    super.close()
    masterDataCache.close()
  }

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.successEventCount, config.systemEventCount, config.totalEventCount, config.successInsertCount, config.successUpdateCount, config.failedCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }
  override def process(datasetId: String, context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, elements: lang.Iterable[mutable.Map[String, AnyRef]], metrics: Metrics): Unit = {

    implicit val jsonFormats: Formats = DefaultFormats.withLong

    implicit class JsonHelper(json: JValue) {
      def customExtract[T](path: String)(implicit mf: Manifest[T]): T = {
        path.split('.').foldLeft(json)({ case (acc: JValue, node: String) => acc \ node }).extract[T]
      }
    }

    val eventsList = elements.asScala.toList
    metrics.incCounter(datasetId, config.totalEventCount, eventsList.size.toLong)
    val dataset = DatasetRegistry.getDataset(datasetId).get
    val eventsMap = eventsList.map(msg => {
      val json = parse(JSONUtil.serialize(msg(config.CONST_EVENT)), useBigIntForLong = false)
      val key = json.customExtract[String](dataset.datasetConfig.key)
      if (key == null) {
        metrics.incCounter(datasetId, config.failedCount)
        context.output(config.failedEventsTag, markEventFailed(datasetId, msg, ErrorConstants.MISSING_DATASET_CONFIG_KEY, msg(config.CONST_OBSRV_META).asInstanceOf[Map[String, AnyRef]]))
      }
      (key, json)
    }).toMap
    val validEventsMap = eventsMap.filter(f => f._1 != null)
    val result = masterDataCache.process(dataset, validEventsMap)
    metrics.incCounter(datasetId, config.successInsertCount, result._1)
    metrics.incCounter(datasetId, config.successUpdateCount, result._2)
    metrics.incCounter(datasetId, config.successEventCount, eventsList.size.toLong)

    eventsList.foreach(event => {
      event.remove(config.CONST_EVENT)
      context.output(config.successTag(), markComplete(event, dataset.dataVersion))
    })
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

  private def createWrapperEvent(dataset: String, event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    mutable.Map(config.CONST_DATASET -> dataset, config.CONST_EVENT -> JSONUtil.serialize(event.toMap))
  }

  private def updateEvent(event: mutable.Map[String, AnyRef], obsrvMeta: Map[String, AnyRef]) = {
    event.put(config.CONST_OBSRV_META, obsrvMeta)
  }
}
