package org.sunbird.obsrv.function

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.{ErrorConstants, FunctionalError, Producer}
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.pipeline.task.CacheIndexerConfig
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction
import org.sunbird.obsrv.util.MasterDataCache

import scala.collection.mutable

class MasterDataProcessorFunction(config: CacheIndexerConfig) extends BaseDatasetProcessFunction(config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[MasterDataProcessorFunction])
  private[this] var masterDataCache: MasterDataCache = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    masterDataCache = new MasterDataCache(config)
    masterDataCache.open(DatasetRegistry.getAllDatasets(Some("master")))
  }

  override def close(): Unit = {
    super.close()
    masterDataCache.close()
  }

  override def getMetrics(): List[String] = {
    List(config.successEventCount, config.systemEventCount, config.totalEventCount, config.successInsertCount, config.successUpdateCount)
  }

  override def processElement(dataset: Dataset, msg: mutable.Map[String, AnyRef], context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context, metrics: Metrics): Unit = {

    metrics.incCounter(dataset.id, config.totalEventCount)
    masterDataCache.open(dataset)
    val event = JSONUtil.serialize(msg(config.CONST_EVENT))
    val json = parse(event, useBigIntForLong = false)
    val node = JSONUtil.getKey(dataset.datasetConfig.keysConfig.dataKey.get, event)
    if (node.isMissingNode) {
      markFailure(Some(dataset.id), msg, context, metrics, ErrorConstants.MISSING_DATASET_CONFIG_KEY, Producer.masterdataprocessor, FunctionalError.MissingMasterDatasetKey, datasetType = Some(dataset.datasetType))
    } else {
      val result = masterDataCache.process(dataset, node.asText(), json)
      metrics.incCounter(dataset.id, config.successInsertCount, result._1)
      metrics.incCounter(dataset.id, config.successUpdateCount, result._2)
      metrics.incCounter(dataset.id, config.successEventCount)
    }

  }

}