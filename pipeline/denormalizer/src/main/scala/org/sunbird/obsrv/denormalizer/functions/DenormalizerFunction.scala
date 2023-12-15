package org.sunbird.obsrv.denormalizer.functions

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.Models._
import org.sunbird.obsrv.core.model.Producer.Producer
import org.sunbird.obsrv.core.model.StatusCode.StatusCode
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.{JSONUtil, Util}
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.denormalizer.util.{DenormCache, DenormEvent}
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction

import scala.collection.mutable

class DenormalizerFunction(config: DenormalizerConfig) extends BaseDatasetProcessFunction(config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormalizerFunction])

  private[this] var denormCache: DenormCache = _

  override def getMetrics(): List[String] = {
    List(config.denormSuccess, config.denormTotal, config.denormFailed, config.eventsSkipped, config.denormPartialSuccess)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    denormCache = new DenormCache(config)
    denormCache.open(DatasetRegistry.getAllDatasets(config.datasetType()))
  }

  override def close(): Unit = {
    super.close()
    denormCache.close()
  }

  override def processElement(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(dataset.id, config.denormTotal)
    denormCache.open(dataset)
    if (dataset.denormConfig.isDefined) {
      val event = DenormEvent(msg)
      val denormEvent = denormCache.denormEvent(dataset.id, event, dataset.denormConfig.get.denormFields)
      val status = getDenormStatus(denormEvent)
      context.output(config.denormEventsTag, markStatus(denormEvent.msg, Producer.denorm, status))
      status match {
        case StatusCode.success => metrics.incCounter(dataset.id, config.denormSuccess)
        case _ =>
          metrics.incCounter(dataset.id, if (status == StatusCode.partial) config.denormPartialSuccess else config.denormFailed)
          generateSystemEvent(dataset, denormEvent, context)
          logData(dataset.id, denormEvent)
      }
    } else {
      metrics.incCounter(dataset.id, config.eventsSkipped)
      context.output(config.denormEventsTag, markSkipped(msg, Producer.denorm))
    }
  }

  private def logData(datasetId: String, denormEvent: DenormEvent): Unit = {
    logger.warn(s"Denormalizer | Denorm operation is not successful | dataset=$datasetId | denormStatus=${JSONUtil.serialize(denormEvent.fieldStatus)}")
  }

  private def generateSystemEvent(dataset: Dataset, denormEvent: DenormEvent, context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context): Unit = {

    denormEvent.fieldStatus.filter(f => !f._2.success).groupBy(f => f._2.error.get).map(f => (f._1, f._2.size))
      .foreach(f => {
        val functionalError = f._1 match {
          case ErrorConstants.DENORM_KEY_MISSING => FunctionalError.DenormKeyMissing
          case ErrorConstants.DENORM_KEY_NOT_A_STRING_OR_NUMBER => FunctionalError.DenormKeyInvalid
          case ErrorConstants.DENORM_DATA_NOT_FOUND => FunctionalError.DenormDataNotFound
        }
        context.output(config.systemEventsOutputTag, JSONUtil.serialize(SystemEvent(
          EventID.METRIC,
          ctx = ContextData(module = ModuleID.processing, pdata = PData(config.jobName, PDataType.flink, Some(Producer.denorm)), dataset = Some(dataset.id), dataset_type = Some(dataset.datasetType)),
          data = EData(error = Some(ErrorLog(pdata_id = Producer.denorm, pdata_status = StatusCode.failed, error_type = functionalError, error_code = f._1.errorCode, error_message = f._1.errorMsg, error_level = ErrorLevel.critical, error_count = Some(f._2))))
        )))
      })
  }

  private def getDenormStatus(denormEvent: DenormEvent): StatusCode = {
    val totalFieldsCount = denormEvent.fieldStatus.size
    val successCount = denormEvent.fieldStatus.values.count(f => f.success)
    if (totalFieldsCount == successCount) StatusCode.success else if (successCount > 0) StatusCode.partial else StatusCode.failed

  }

  private def markStatus(event: mutable.Map[String, AnyRef], producer: Producer, status: StatusCode): mutable.Map[String, AnyRef] = {
    val obsrvMeta = Util.getMutableMap(event("obsrv_meta").asInstanceOf[Map[String, AnyRef]])
    addFlags(obsrvMeta, Map(producer.toString -> status.toString))
    addTimespan(obsrvMeta, producer)
    event.put("obsrv_meta", obsrvMeta.toMap)
    event
  }

}
