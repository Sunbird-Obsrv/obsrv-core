package org.sunbird.obsrv.transformer.types

import co.com.bancolombia.datamask.{MaskUtils => CustomMaskUtils}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.MissingNode
import org.json4s.{DefaultFormats, Formats, JValue, MappingException}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.DatasetTransformation

import java.util.regex.Pattern

class MaskTransformer extends ITransformer[String] {

  implicit val jsonFormats: Formats = DefaultFormats.withLong
  private val logger = LoggerFactory.getLogger(classOf[EncryptTransformer])

  implicit class JsonHelper(json: JValue) {
    def customExtract[T](path: String)(implicit mf: Manifest[T]): T = {
      path.split('.').foldLeft(json)({ case (acc: JValue, node: String) => acc \ node }).extract[T]
    }
  }

  private val maskRatio = 0.35 // TODO: Move it to a config
  private val emailPattern = Pattern.compile("^(.+)@(\\S+)$") // TODO: Read the pattern from config

  private def mask(value: String): String = {
    if (value.isEmpty) return value
    if (emailPattern.matcher(value).matches()) {
      CustomMaskUtils.maskAsEmail(value)
    } else {
      val openDigits = (value.length * maskRatio).ceil
      val firstDigitCount = (openDigits / 2).floor
      val lastDigitCount = openDigits - firstDigitCount
      CustomMaskUtils.mask(value, firstDigitCount.intValue(), lastDigitCount.intValue())
    }
  }

  override def transformField(json: JValue, jsonNode: JsonNode, dt: DatasetTransformation): (JValue, TransformFieldStatus) = {
    val emptyNode = getJSON(dt.fieldKey, MissingNode.getInstance())
    try {
      val currentValue = json.customExtract[String](dt.transformationFunction.expr)
      val maskedValue = mask(currentValue).replaceAll("\"", "")
      (getJSON(dt.fieldKey, maskedValue), TransformFieldStatus(dt.fieldKey, dt.transformationFunction.expr, success = true, dt.mode.get))
    } catch {
      case ex: MappingException =>
        logger.error(s"Transformer(Mask) | Exception parsing transformation expression | Data=${JSONUtil.serialize(dt)} | error=${ex.getMessage}", ex)
        (emptyNode, TransformFieldStatus(dt.fieldKey, dt.transformationFunction.expr, success = false, dt.mode.get, Some(ErrorConstants.TRANSFORMATION_FIELD_MISSING)))
    }
  }

}

object MaskTransformer {

  private val maskingTransformer = new MaskTransformer()

  def transform(json: JValue, jsonNode: JsonNode, dtList: List[DatasetTransformation]): TransformationResult = {
    maskingTransformer.transform(json, jsonNode, dtList)
  }

}