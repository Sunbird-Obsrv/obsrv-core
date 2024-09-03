package org.sunbird.obsrv.transformer.types

import com.api.jsonata4java.expressions.{EvaluateException, Expressions, ParseException}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.MissingNode
import org.json4s.JValue
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels
import org.sunbird.obsrv.model.DatasetModels.TransformationFunction

class JSONAtaTransformer extends ITransformer {

  private val logger = LoggerFactory.getLogger(classOf[JSONAtaTransformer])

  override def transformField(json: JValue, jsonNode: JsonNode, dt: DatasetModels.DatasetTransformation): (JValue, TransformFieldStatus) = {
    val emptyNode = getJSON(dt.fieldKey, MissingNode.getInstance())
    try {
      val expr = Expressions.parse(dt.transformationFunction.expr)
      val resNode = expr.evaluate(jsonNode)
      (Option(resNode).map { node => getJSON(dt.fieldKey, node) }.getOrElse(emptyNode), TransformFieldStatus(dt.fieldKey, dt.transformationFunction.expr, success = true, dt.mode.get))
    } catch {
      case ex1: ParseException =>
        logger.error(s"Transformer(JSONATA) | Exception parsing transformation expression | Data=${JSONUtil.serialize(dt)} | error=${ex1.getMessage}", ex1)
        (emptyNode, TransformFieldStatus(dt.fieldKey, dt.transformationFunction.expr, success = false, dt.mode.get, Some(ErrorConstants.INVALID_EXPR_FUNCTION)))
      case ex2: EvaluateException =>
        logger.error(s"Transformer(JSONATA) | Exception evaluating transformation expression | Data=${JSONUtil.serialize(dt)} | error=${ex2.getMessage}", ex2)
        (emptyNode, TransformFieldStatus(dt.fieldKey, dt.transformationFunction.expr, success = false, dt.mode.get, Some(ErrorConstants.ERR_EVAL_EXPR_FUNCTION)))
      case ex3: Exception =>
        logger.error(s"Transformer(JSONATA) | Unknown error | Data=${JSONUtil.serialize(dt)} | error=${ex3.getMessage}", ex3)
        (emptyNode, TransformFieldStatus(dt.fieldKey, dt.transformationFunction.expr, success = false, dt.mode.get, Some(ErrorConstants.ERR_UNKNOWN_TRANSFORM_EXCEPTION)))
    }
  }

  def evaluate(jsonNode: JsonNode, tf: TransformationFunction): JsonNode = {

    try {
      val expr = Expressions.parse(tf.expr)
      expr.evaluate(jsonNode)
    } catch {
      case ex1: ParseException =>
        logger.error(s"Transformer(JSONATA) | Exception parsing transformation expression | Data=${JSONUtil.serialize(tf)} | error=${ex1.getMessage}", ex1)
        MissingNode.getInstance()
      case ex2: EvaluateException =>
        logger.error(s"Transformer(JSONATA) | Exception evaluating transformation expression | Data=${JSONUtil.serialize(tf)} | error=${ex2.getMessage}", ex2)
        MissingNode.getInstance()
      case ex3: Exception =>
        logger.error(s"Transformer(JSONATA) | Unknown error | Data=${JSONUtil.serialize(tf)} | error=${ex3.getMessage}", ex3)
        MissingNode.getInstance()
    }
  }
}

object JSONAtaTransformer {

  private val jsonAtaTransformer = new JSONAtaTransformer()

  def transform(json: JValue, jsonNode: JsonNode, dtList: List[DatasetModels.DatasetTransformation]): TransformationResult = {
    jsonAtaTransformer.transform(json, jsonNode, dtList)
  }

  def evaluate(jsonNode: JsonNode, transformation: TransformationFunction): JsonNode = {
    jsonAtaTransformer.evaluate(jsonNode, transformation)
  }

}