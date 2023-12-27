package org.sunbird.obsrv.pipeline.function

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.{ErrorConstants, FunctionalError, Producer}
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.pipeline.task.MasterDataProcessorConfig
import org.sunbird.obsrv.pipeline.util.MasterDataCache
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.streaming.BaseDatasetWindowProcessFunction

import scala.collection.mutable

class MasterDataProcessorFunction(config: MasterDataProcessorConfig) extends BaseDatasetWindowProcessFunction(config) {

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

  override def getMetrics(): List[String] = {
    List(config.successEventCount, config.systemEventCount, config.totalEventCount, config.successInsertCount, config.successUpdateCount)
  }

  override def processWindow(dataset: Dataset, context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, elements: List[mutable.Map[String, AnyRef]], metrics: Metrics): Unit = {

    metrics.incCounter(dataset.id, config.totalEventCount, elements.size.toLong)
    masterDataCache.open(dataset)
    val eventsMap = elements.map(msg => {
      val event = JSONUtil.serialize(msg(config.CONST_EVENT))
      val json = parse(event, useBigIntForLong = false)
      val node = JSONUtil.getKey(dataset.datasetConfig.key, event)
      if (node.isMissingNode) {
        markFailure(Some(dataset.id), msg, context, metrics, ErrorConstants.MISSING_DATASET_CONFIG_KEY, Producer.masterdataprocessor, FunctionalError.MissingMasterDatasetKey, datasetType = Some(dataset.datasetType))
      }
      (node.asText(), json)
    }).toMap
    val validEventsMap = eventsMap.filter(f => f._1.nonEmpty)
    val result = masterDataCache.process(dataset, validEventsMap)
    metrics.incCounter(dataset.id, config.successInsertCount, result._1)
    metrics.incCounter(dataset.id, config.successUpdateCount, result._2)
    metrics.incCounter(dataset.id, config.successEventCount, validEventsMap.size.toLong)

    elements.foreach(event => {
      event.remove(config.CONST_EVENT)
      markCompletion(dataset, super.markComplete(event, dataset.dataVersion), context, Producer.masterdataprocessor)
    })
  }

}