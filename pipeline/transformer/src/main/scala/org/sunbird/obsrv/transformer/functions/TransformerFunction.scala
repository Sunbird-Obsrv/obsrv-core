package org.sunbird.obsrv.transformer.functions

import com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.obsrv.transformer.task.TransformerConfig
import org.sunbird.obsrv.transformer.types._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.Models._
import org.sunbird.obsrv.core.model.StatusCode.StatusCode
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.{Dataset, DatasetTransformation}
import org.sunbird.obsrv.model.TransformMode
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.streaming.BaseDatasetProcessFunction

import scala.collection.mutable

case class TransformationStatus(resultJson: JValue, status: StatusCode, fieldStatus: List[TransformFieldStatus])

class TransformerFunction(config: TransformerConfig) extends BaseDatasetProcessFunction(config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[TransformerFunction])

  override def getMetrics(): List[String] = {
    List(config.totalEventCount, config.transformSuccessCount, config.transformPartialCount, config.transformFailedCount, config.transformSkippedCount)
  }

  /**
   * Method to process the event transformations
   */
  override def processElement(dataset: Dataset, msg: mutable.Map[String, AnyRef], context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context, metrics: Metrics): Unit = {

    implicit val jsonFormats: Formats = DefaultFormats.withLong
    val result = TransformerFunctionHelper.processTransformation(dataset, msg, config)
    metrics.incCounter(dataset.id, config.totalEventCount)
    msg.put(config.CONST_EVENT, result.resultJson.extract[Map[String, AnyRef]])
    result.status match {
      case StatusCode.skipped =>
        metrics.incCounter(dataset.id, config.transformSkippedCount)
        context.output(config.transformerOutputTag, markSkipped(msg, Producer.transformer))
      case StatusCode.failed =>
        metrics.incCounter(dataset.id, config.transformFailedCount)
        context.output(config.transformerFailedOutputTag, markFailed(msg, ErrorConstants.ERR_TRANSFORMATION_FAILED, Producer.transformer))
        logSystemEvents(dataset, result, context)
      case StatusCode.partial =>
        metrics.incCounter(dataset.id, config.transformPartialCount)
        context.output(config.transformerOutputTag, markPartial(msg, Producer.transformer))
        logSystemEvents(dataset, result, context)
      case StatusCode.success =>
        metrics.incCounter(dataset.id, config.transformSuccessCount)
        context.output(config.transformerOutputTag, markSuccess(msg, Producer.transformer))
    }
  }

  private def logSystemEvents(dataset: Dataset, result: TransformationStatus, ctx: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context): Unit = {
    result.fieldStatus.filter(p => !p.success).groupBy(f => f.error.get).map(f => (f._1, f._2.size))
      .foreach(errCount => {
        val err = errCount._1
        val functionalError = err match {
          case ErrorConstants.INVALID_EXPR_FUNCTION => FunctionalError.TransformParseError
          case ErrorConstants.ERR_EVAL_EXPR_FUNCTION => FunctionalError.TransformEvalError
          case ErrorConstants.ERR_UNKNOWN_TRANSFORM_EXCEPTION => FunctionalError.TransformFailedError
          case ErrorConstants.TRANSFORMATION_FIELD_MISSING => FunctionalError.TransformFieldMissing
        }

        ctx.output(config.systemEventsOutputTag, JSONUtil.serialize(SystemEvent(
          EventID.METRIC,
          ctx = ContextData(module = ModuleID.processing, pdata = PData(config.jobName, PDataType.flink, Some(Producer.denorm)), dataset = Some(dataset.id), dataset_type = Some(dataset.datasetType)),
          data = EData(error = Some(ErrorLog(pdata_id = Producer.denorm, pdata_status = StatusCode.failed, error_type = functionalError, error_code = err.errorCode, error_message = err.errorMsg, error_level = ErrorLevel.critical, error_count = Some(errCount._2))))
        )))
      })

    logger.warn(s"Transformer | Transform operation is not successful | dataset=${dataset.id} | TransformStatusData=${JSONUtil.serialize(result.fieldStatus)}")
  }

}

object TransformerFunctionHelper {

  implicit val jsonFormats: Formats = DefaultFormats.withLong
  private val mapper = new ObjectMapper()

  implicit class JsonHelper(json: JValue) {
    def customExtract[T](path: String)(implicit mf: Manifest[T]): T = {
      path.split('.').foldLeft(json)({ case (acc: JValue, node: String) => acc \ node }).extract[T]
    }
  }

  @throws[ObsrvException]
  def processTransformation(dataset: Dataset, msg: mutable.Map[String, AnyRef], config: TransformerConfig): TransformationStatus = {

    val event = JSONUtil.serialize(msg(config.CONST_EVENT))
    val json = parse(event, useBigIntForLong = false)
    val datasetTransformations = DatasetRegistry.getDatasetTransformations(dataset.id)
    processTransformations(json, datasetTransformations)
  }

  def processTransformations(json: JValue, datasetTransformations: Option[List[DatasetTransformation]]): TransformationStatus = {
    if (datasetTransformations.isDefined) {
      val result = applyTransformations(json, datasetTransformations.get)
      TransformationStatus(json merge result.json, getStatus(result.fieldStatus), result.fieldStatus)
    } else {
      TransformationStatus(json, StatusCode.skipped, List[TransformFieldStatus]())
    }
  }

  private def getStatus(fieldStatus: List[TransformFieldStatus]): StatusCode = {
    val failedCount = fieldStatus.count(p => p.mode == TransformMode.Strict && !p.success)
    val partialCount = fieldStatus.count(p => p.mode == TransformMode.Lenient && !p.success)
    if (failedCount > 0) StatusCode.failed else if (partialCount > 0) StatusCode.partial else StatusCode.success

  }

  private def applyTransformations(json: JValue, datasetTransformations: List[DatasetTransformation]): TransformationResult = {
    datasetTransformations.groupBy(f => f.transformationFunction.`type`).mapValues(f => {
      applyTransformation(f.head.transformationFunction.`type`, json, f)
    }).values.reduceLeft((a, b) => TransformationResult(mergeJson(a, b), mergeStatus(a, b)))
  }

  private def mergeJson(a: TransformationResult, b: TransformationResult): JValue = {
    a.json merge b.json
  }

  private def mergeStatus(a: TransformationResult, b: TransformationResult): List[TransformFieldStatus] = {
    a.fieldStatus ++ b.fieldStatus
  }

  private def applyTransformation(tfType: String, json: JValue, dt: List[DatasetTransformation]): TransformationResult = {
    val jsonNode = mapper.readTree(compact(render(json)))
    tfType match {
      case "mask" => MaskTransformer.transform(json, jsonNode, dt)
      case "jsonata" => JSONAtaTransformer.transform(json, jsonNode, dt)
      case "encrypt" => EncryptTransformer.transform(json, jsonNode, dt)
      case _ => TransformationResult(json, List[TransformFieldStatus]())
    }
  }
}