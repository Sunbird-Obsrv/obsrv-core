package org.sunbird.obsrv.transformer.types

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import org.json4s.native.JsonMethods.parse
import org.json4s.{JNothing, JObject, JValue}
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.model.DatasetModels.DatasetTransformation
import org.sunbird.obsrv.model.TransformMode.TransformMode
import org.sunbird.obsrv.transformer.util.ConditionEvaluator

import scala.collection.mutable.ListBuffer

case class TransformFieldStatus(fieldKey: String, expr: String, success: Boolean, mode: TransformMode, error: Option[Error] = None)
case class TransformationResult(json: JValue, fieldStatus: List[TransformFieldStatus])
abstract class ITransformer[T] {

  def transformField(json: JValue, jsonNode: JsonNode, dt: DatasetTransformation): (JValue, TransformFieldStatus)

  def transform(json: JValue, jsonNode: JsonNode, dtList: List[DatasetTransformation]): TransformationResult = {
    val resultBuffer = ListBuffer[TransformFieldStatus]()
    val evalList = dtList.map(dt => {
      val conditionStatus = ConditionEvaluator.evalCondition(dt.datasetId, jsonNode, dt.transformationFunction.condition, dt.mode)
      if (!conditionStatus.success) {
        resultBuffer.append(TransformFieldStatus(dt.fieldKey, conditionStatus.expr, success = false, dt.mode.get, conditionStatus.error))
        JObject(dt.fieldKey -> JNothing)
      } else {
        val result = transformField(json, jsonNode, dt)
        resultBuffer.append(result._2)
        result._1
      }
    })
    val transformedJson = evalList.reduceLeftOption((a, b) => a merge b).getOrElse(JNothing)
    TransformationResult(transformedJson, resultBuffer.toList)
  }

  def getJSON(key: String, value: String): JValue = {
    val path = key.split('.').toList ++ List(s""""$value"""")
    val outPath = path.reduceRight((a, b) => s"""{"$a":$b}""")
    parse(outPath, useBigIntForLong = false)
  }

  def getJSON(key: String, value: AnyRef): JValue = {
    val path = key.split('.').toList ++ List(s"""$value""")
    val outPath = path.reduceRight((a, b) => s"""{"$a":$b}""")
    parse(outPath, useBigIntForLong = false)
  }

  def getJSON(key: String, value: JsonNode): JValue = {
    Option(value).map { jsonNodeValue =>
      jsonNodeValue.getNodeType match {
        case JsonNodeType.STRING => getJSON(key, jsonNodeValue.textValue())
        case JsonNodeType.NUMBER => getJSON(key, jsonNodeValue.numberValue().asInstanceOf[AnyRef])
        case JsonNodeType.BOOLEAN => getJSON(key, jsonNodeValue.booleanValue().asInstanceOf[AnyRef])
        case JsonNodeType.ARRAY => getJSON(key, jsonNodeValue.toString.asInstanceOf[AnyRef])
        case JsonNodeType.OBJECT => getJSON(key, jsonNodeValue.toString.asInstanceOf[AnyRef])
        case _ => getJSON(key, null.asInstanceOf[AnyRef])
      }
    }.getOrElse(getJSON(key, null.asInstanceOf[AnyRef]))
  }

}