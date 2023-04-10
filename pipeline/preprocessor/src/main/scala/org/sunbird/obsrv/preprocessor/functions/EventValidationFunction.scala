package org.sunbird.obsrv.preprocessor.functions

import com.github.fge.jsonschema.core.report.ProcessingReport
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.Models.{PData, SystemEvent}
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.preprocessor.util.SchemaValidator
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

class EventValidationFunction(config: PipelinePreprocessorConfig,
                              @transient var schemaValidator: SchemaValidator = null)
                             (implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) {
  private[this] val logger = LoggerFactory.getLogger(classOf[EventValidationFunction])

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.validationTotalMetricsCount, config.validationFailureMetricsCount,
      config.validationSuccessMetricsCount, config.validationSkipMetricsCount, config.eventFailedMetricsCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (schemaValidator == null) {
      schemaValidator = new SchemaValidator(config)
      schemaValidator.loadDataSchemas(DatasetRegistry.getAllDatasets(config.datasetType()))
    }
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(config.defaultDatasetID, config.validationTotalMetricsCount)
    val datasetId = msg.get(config.CONST_DATASET)
    if (datasetId.isEmpty) {
      context.output(config.failedEventsOutputTag, markFailed(msg, ErrorConstants.MISSING_DATASET_ID, config.jobName))
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return
    }
    val datasetOpt = DatasetRegistry.getDataset(datasetId.get.asInstanceOf[String])
    if (datasetOpt.isEmpty) {
      context.output(config.failedEventsOutputTag, markFailed(msg, ErrorConstants.MISSING_DATASET_CONFIGURATION, config.jobName))
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return
    }
    val dataset = datasetOpt.get
    if (!super.containsEvent(msg)) {
      metrics.incCounter(dataset.id, config.eventFailedMetricsCount)
      context.output(config.failedEventsOutputTag, markFailed(msg, ErrorConstants.EVENT_MISSING, config.jobName))
      return
    }
    val validationConfig = dataset.validationConfig
    if (validationConfig.isDefined && validationConfig.get.validate.get) {
      validateEvent(dataset, msg, context, metrics)
    } else {
      metrics.incCounter(dataset.id, config.validationSkipMetricsCount)
      context.output(config.validEventsOutputTag, markSkipped(msg, "EventValidation"))
    }
  }

  private def validateEvent(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                            context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                            metrics: Metrics): Unit = {

    val event = msg(config.CONST_EVENT).asInstanceOf[Map[String, AnyRef]]
    try {
      if (schemaValidator.schemaFileExists(dataset)) {
        val validationReport = schemaValidator.validate(dataset.id, event)
        if (validationReport.isSuccess) {
          onValidationSuccess(dataset, msg, metrics, context)
        } else {
          onValidationFailure(dataset, msg, metrics, context, validationReport)
        }
      }
    } catch {
      case ex: ObsrvException =>
        metrics.incCounter(dataset.id, config.validationFailureMetricsCount)
        context.output(config.failedEventsOutputTag, markFailed(msg, ex.error, "EventValidation"))
    }
  }

  private def onValidationSuccess(dataset: Dataset, event: mutable.Map[String, AnyRef], metrics: Metrics,
                                  context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context): Unit = {
    metrics.incCounter(dataset.id, config.validationSuccessMetricsCount)
    context.output(config.validEventsOutputTag, markSuccess(event, "EventValidation"))
  }

  private def onValidationFailure(dataset: Dataset, event: mutable.Map[String, AnyRef], metrics: Metrics,
                                  context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                                  validationReport: ProcessingReport): Unit = {
    val failedErrorMsg = schemaValidator.getInvalidFieldName(validationReport.toString)
    metrics.incCounter(dataset.id, config.validationFailureMetricsCount)
    context.output(config.invalidEventsOutputTag, markFailed(event, ErrorConstants.SCHEMA_VALIDATION_FAILED, "EventValidation"))
    val systemEvent = SystemEvent(PData(config.jobName, "flink", "validation"), Map("error_code" -> ErrorConstants.SCHEMA_VALIDATION_FAILED.errorCode, "error_msg" -> failedErrorMsg))
    context.output(config.systemEventsOutputTag, JSONUtil.serialize(systemEvent))
  }

}