package org.sunbird.obsrv.preprocessor.functions

import com.github.fge.jsonschema.core.report.ProcessingReport
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.ErrorConstants.Error
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
      config.validationSuccessMetricsCount, config.validationSkipMetricsCount)
    MetricsList(DatasetRegistry.getDataSetIds(), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (schemaValidator == null) {
      schemaValidator = new SchemaValidator(config)
    }
  }

  override def close(): Unit = {
    super.close()
  }

  override def processElement(msg: mutable.Map[String, AnyRef],
                              context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {

    metrics.incCounter(config.defaultDatasetID, config.validationTotalMetricsCount)
    val datasetId = msg.get("dataset")
    if (datasetId.isEmpty) {
      markFailed(msg, ErrorConstants.MISSING_DATASET_ID)
      context.output(config.failedEventsOutputTag, msg)
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return
    }
    val datasetOpt = DatasetRegistry.getDataset(datasetId.get.asInstanceOf[String])
    if (datasetOpt.isEmpty) {
      markFailed(msg, ErrorConstants.MISSING_DATASET_CONFIGURATION)
      context.output(config.failedEventsOutputTag, msg)
      metrics.incCounter(config.defaultDatasetID, config.eventFailedMetricsCount)
      return
    }
    val dataset = datasetOpt.get
    val validationConfig = dataset.validationConfig
    if (validationConfig.isDefined && validationConfig.get.validate.get) {
      validateEvent(dataset, msg, context, metrics)
    } else {
      metrics.incCounter(dataset.id, config.validationSkipMetricsCount)
      context.output(config.validEventsOutputTag, msg)
    }
  }

  private def validateEvent(dataset: Dataset, msg: mutable.Map[String, AnyRef],
                            context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                            metrics: Metrics): Unit = {

    val event = msg("event").asInstanceOf[Map[String, AnyRef]]
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
        markFailed(msg, ex.error)
        metrics.incCounter(dataset.id, config.validationFailureMetricsCount)
        context.output(config.failedEventsOutputTag, msg)
    }
  }

  private def onValidationSuccess(dataset: Dataset, event: mutable.Map[String, AnyRef], metrics: Metrics,
                                  context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context): Unit = {
    metrics.incCounter(dataset.id, config.validationSuccessMetricsCount)
    context.output(config.validEventsOutputTag, markSuccess(event))
  }

  private def onValidationFailure(dataset: Dataset, event: mutable.Map[String, AnyRef], metrics: Metrics,
                                  context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                                  validationReport: ProcessingReport): Unit = {
    val failedErrorMsg = schemaValidator.getInvalidFieldName(validationReport.toString)
    metrics.incCounter(dataset.id, config.validationFailureMetricsCount)
    context.output(config.invalidEventsOutputTag, markFailed(event, ErrorConstants.SCHEMA_VALIDATION_FAILED))
    val systemEvent = SystemEvent(PData(config.jobName, "flink", "validation"), Map("error_code" -> ErrorConstants.SCHEMA_VALIDATION_FAILED.errorCode, "error_msg" -> failedErrorMsg))
    context.output(config.systemEventsOutputTag, JSONUtil.serialize(systemEvent))
  }

  private def markFailed(event: mutable.Map[String, AnyRef], error: Error): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("preprocessing_processed" -> "no"))
    addError(event, Map("src" -> config.jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    event
  }

  private def markSuccess(event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("preprocessing_processed" -> "yes"))
    event
  }

}