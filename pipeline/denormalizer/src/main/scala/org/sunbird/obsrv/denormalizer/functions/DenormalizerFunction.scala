package org.sunbird.obsrv.denormalizer.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.denormalizer.util.DenormCache
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

class DenormalizerFunction(config: DenormalizerConfig)(implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormalizerFunction])

  private[this] var denormCache: DenormCache = _

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.denormSuccess, config.denormTotal, config.denormFailed, config.eventsSkipped)
    MetricsList(DatasetRegistry.getDataSetIds(), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    denormCache = new DenormCache(config)
    denormCache.open(DatasetRegistry.getAllDatasets())
  }

  override def close(): Unit = {
    super.close()
    denormCache.close()
  }

  override def processElement(msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    val datasetId = msg("dataset").asInstanceOf[String] // DatasetId cannot be empty at this stage
    metrics.incCounter(datasetId, config.denormTotal)
    val dataset = DatasetRegistry.getDataset(datasetId).get
    val event = getMutableMap(msg("event").asInstanceOf[Map[String, AnyRef]])

    if (dataset.denormConfig.isDefined) {
      try {
        msg.put("event", denormCache.denormEvent(datasetId, event, dataset.denormConfig.get.denormFields))
        metrics.incCounter(datasetId, config.denormSuccess)
        context.output(config.denormEventsTag, markSuccess(msg))
      } catch {
        case ex: ObsrvException =>
          metrics.incCounter(datasetId, config.denormFailed)
          context.output(config.denormFailedTag, markFailed(msg, ex.error))
      }
    } else {
      metrics.incCounter(datasetId, config.eventsSkipped)
      context.output(config.denormEventsTag, markSkipped(msg))
    }

  }

  private def markSkipped(event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("denorm_processed" -> "skipped"))
    event
  }

  private def markFailed(event: mutable.Map[String, AnyRef], error: Error): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("denorm_processed" -> "no"))
    addError(event, Map("src" -> config.jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    event
  }

  private def markSuccess(event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("denorm_processed" -> "yes"))
    event
  }
}
