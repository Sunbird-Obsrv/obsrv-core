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
  private[this] val schemaMap = mutable.Map[String, (JsonSchema, Boolean)]()

  def loadDataSchemas(datasets: List[Dataset]) = {
    datasets.foreach(dataset => {
      if(dataset.jsonSchema.isDefined) {
        try {
          loadJsonSchema(dataset.id, dataset.jsonSchema.get)
        } catch {
          case ex: ObsrvException => ex.printStackTrace()
            schemaMap.put(dataset.id, (null, false))
        }
      }
    })
  }

  private def loadJsonSchema(datasetId: String, jsonSchemaStr: String) = {
    val schemaFactory = JsonSchemaFactory.byDefault
    try {
      val jsonSchema = schemaFactory.getJsonSchema(JsonLoader.fromString(jsonSchemaStr))
      schemaMap.put(datasetId, (jsonSchema, true))
    } catch {
      case ex: Exception =>
        logger.error("SchemaValidator:loadJsonSchema() - Exception", ex)
        throw new ObsrvException(ErrorConstants.INVALID_JSON_SCHEMA.copy(errorReason = ex.getMessage))
    }
  }

  def schemaFileExists(dataset: Dataset): Boolean = {

    if (dataset.jsonSchema.isEmpty) {
      throw new ObsrvException(ErrorConstants.JSON_SCHEMA_NOT_FOUND)
    }
    schemaMap.get(dataset.id).map(f => f._2).orElse(Some(false)).get
  }

  @throws[IOException]
  @throws[ProcessingException]
  def validate(datasetId: String, event: Map[String, AnyRef]): ProcessingReport = {
    schemaMap(datasetId)._1.validate(JSONUtil.convertValue(event))
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
