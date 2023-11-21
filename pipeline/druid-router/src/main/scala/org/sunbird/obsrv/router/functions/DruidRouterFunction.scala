package org.sunbird.obsrv.router.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.Util
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.router.task.DruidRouterConfig

import scala.collection.mutable

class DruidRouterFunction(config: DruidRouterConfig) extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DruidRouterFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.routerTotalCount, config.routerSuccessCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  override def processElement(msg: mutable.Map[String, AnyRef],
                              ctx: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
    try {
      implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
      val datasetId = msg(config.CONST_DATASET).asInstanceOf[String] // DatasetId cannot be empty at this stage
      metrics.incCounter(datasetId, config.routerTotalCount)
      val dataset = DatasetRegistry.getDataset(datasetId).get
      val event = Util.getMutableMap(msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]])
      event.put(config.CONST_OBSRV_META, msg(config.CONST_OBSRV_META))
      val routerConfig = dataset.routerConfig
      ctx.output(OutputTag[mutable.Map[String, AnyRef]](routerConfig.topic), event)
      metrics.incCounter(datasetId, config.routerSuccessCount)

      msg.remove(config.CONST_EVENT)
      ctx.output(config.statsOutputTag, markComplete(msg, dataset.dataVersion))
    } catch {
      case ex: Exception =>
        logger.error("DruidRouterFunction:processElement() - Exception: ", ex.getMessage)
        ex.printStackTrace()
    }
  }
}
