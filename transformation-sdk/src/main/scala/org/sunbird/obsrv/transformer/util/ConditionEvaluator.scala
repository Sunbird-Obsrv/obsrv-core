package org.sunbird.obsrv.transformer.util

import com.api.jsonata4java.expressions.{EvaluateException, Expressions, ParseException}
import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Condition
import org.sunbird.obsrv.model.TransformMode.TransformMode

case class ConditionStatus(expr: String, success: Boolean, mode: Option[TransformMode] = None, error: Option[Error] = None)
object ConditionEvaluator {

  private val logger = LoggerFactory.getLogger(ConditionEvaluator.getClass)

  def evalCondition(datasetId: String, json: JsonNode, condition: Option[Condition], mode: Option[TransformMode]): ConditionStatus = {
    if(condition.isDefined) {
      condition.get.`type` match {
        case "jsonata" => evalJSONAtaCondition(datasetId, json, condition.get, mode)
        case _ => ConditionStatus("", success = false, mode, Some(ErrorConstants.NO_IMPLEMENTATION_FOUND))
      }
    } else {
      ConditionStatus("", success = true, mode)
    }
  }

  private def evalJSONAtaCondition(datasetId: String, json: JsonNode, condition: Condition, mode: Option[TransformMode]): ConditionStatus = {
    try {
      val expr = Expressions.parse(condition.expr)
      val resultNode = expr.evaluate(json)
      val result = resultNode.isBoolean && resultNode.asBoolean()
      ConditionStatus(condition.expr, result, mode)
    } catch {
      case ex1: ParseException =>
        logger.error(s"Transformer(ConditionEvaluator) | Exception parsing condition expression | dataset=$datasetId | ConditionData=${JSONUtil.serialize(condition)} | error=${ex1.getMessage}", ex1)
        ConditionStatus(condition.expr, success = false, mode, Some(ErrorConstants.INVALID_EXPR_FUNCTION))
      case ex2: EvaluateException =>
        logger.error(s"Transformer(ConditionEvaluator) | Exception evaluating condition expression | dataset=$datasetId | ConditionData=${JSONUtil.serialize(condition)} | error=${ex2.getMessage}", ex2)
        ConditionStatus(condition.expr, success = false, mode, Some(ErrorConstants.ERR_EVAL_EXPR_FUNCTION))
      case ex3: Exception =>
        logger.error(s"Transformer(ConditionEvaluator) | Unknown error during condition evaluation | dataset=$datasetId | ConditionData=${JSONUtil.serialize(condition)} | error=${ex3.getMessage}", ex3)
        ConditionStatus(condition.expr, success = false, mode, Some(ErrorConstants.ERR_UNKNOWN_TRANSFORM_EXCEPTION))
    }
  }

}
