package org.sunbird.obsrv.preprocessor.util

import com.github.fge.jackson.JsonLoader
import com.github.fge.jsonschema.core.exceptions.ProcessingException
import com.github.fge.jsonschema.core.report.ProcessingReport
import com.github.fge.jsonschema.main.{JsonSchema, JsonSchemaFactory}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig

import java.io.IOException
import scala.collection.mutable

class SchemaValidator(config: PipelinePreprocessorConfig) extends java.io.Serializable {

  private val serialVersionUID = 8780940932759659175L
  private[this] val logger = LoggerFactory.getLogger(classOf[SchemaValidator])
  private[this] val schemaMap = mutable.Map[String, JsonSchema]()

  private def loadJsonSchema(datasetId: String, jsonSchemaStr: String) = {
    val schemaFactory = JsonSchemaFactory.byDefault
    try {
      val jsonSchema = schemaFactory.getJsonSchema(JsonLoader.fromString(jsonSchemaStr))
      schemaMap.put(datasetId, jsonSchema)
    } catch {
      case ex: Exception =>
        // TODO: create system event for the error trace
        throw new ObsrvException(ErrorConstants.INVALID_JSON_SCHEMA)
    }
  }

  def schemaFileExists(dataset: Dataset): Boolean = {
    if (schemaMap.contains(dataset.id)) {
      return true
    }
    if (dataset.jsonSchema.isEmpty) {
      throw new ObsrvException(ErrorConstants.JSON_SCHEMA_NOT_FOUND)
    } else {
      loadJsonSchema(dataset.id, dataset.jsonSchema.get)
      true
    }
  }

  @throws[IOException]
  @throws[ProcessingException]
  def validate(datasetId: String, event: Map[String, AnyRef]): ProcessingReport = {
    schemaMap(datasetId).validate(JSONUtil.convertValue(event))
  }

  def getInvalidFieldName(errorInfo: String): String = {
    val message = errorInfo.split("reports:")
    val defaultValidationErrMsg = "Unable to obtain field name for failed validation"
    if (message.length > 1) {
      val fields = message(1).split(",")
      if (fields.length > 2) {
        val pointer = fields(3).split("\"pointer\":")
        pointer(1).substring(0, pointer(1).length - 1)
      } else {
        defaultValidationErrMsg
      }
    } else {
      defaultValidationErrMsg
    }
  }
}
// $COVERAGE-ON$
