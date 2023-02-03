package org.sunbird.obsrv.router.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.router.task.DruidRouterConfig

import scala.collection.mutable

class DruidRouterFunction(config: DruidRouterConfig)
                         (implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DruidRouterFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.routerTotalCount, config.routerSuccessCount)
    MetricsList(DatasetRegistry.getDataSetIds(), metrics)
  }

  override def processElement(msg: mutable.Map[String, AnyRef],
                              ctx: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    val datasetId = msg("dataset").asInstanceOf[String] // DatasetId cannot be empty at this stage
    metrics.incCounter(datasetId, config.routerTotalCount)
    val dataset = DatasetRegistry.getDataset(datasetId).get
    val event = getMutableMap(msg("event").asInstanceOf[Map[String, AnyRef]])
    val routerConfig = dataset.routerConfig
    ctx.output(OutputTag[mutable.Map[String, AnyRef]](routerConfig.topic), event)
    metrics.incCounter(datasetId, config.routerSuccessCount)
  }
}
