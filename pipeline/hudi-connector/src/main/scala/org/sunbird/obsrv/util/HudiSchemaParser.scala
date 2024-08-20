package org.sunbird.obsrv.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonGenerator.Feature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.table.types.logical.{BigIntType, BooleanType, DoubleType, IntType, LogicalType, MapType, RowType, VarCharType, TimestampType, DateType}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.Constants
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.registry.DatasetRegistry
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable


case class HudiSchemaSpec(dataset: String, schema: Schema, inputFormat: InputFormat)
case class Schema(table: String, partitionColumn: String, timestampColumn: String, primaryKey: String, columnSpec: List[ColumnSpec])
case class ColumnSpec(name: String, `type`: String)
case class InputFormat(`type`: String, flattenSpec: Option[JsonFlattenSpec] = None, columns: Option[List[String]] = None)
case class JsonFlattenSpec(fields: List[JsonFieldParserSpec])
case class JsonFieldParserSpec(`type`: String, name: String, expr: Option[String] = None)

class HudiSchemaParser {

  private val logger = LoggerFactory.getLogger(classOf[HudiSchemaParser])

  @transient private val objectMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
    .enable(Feature.WRITE_BIGDECIMAL_AS_PLAIN)
    .build()

  val df = new SimpleDateFormat("yyyy-MM-dd")
  objectMapper.setSerializationInclusion(Include.NON_ABSENT)

  val hudiSchemaMap = new mutable.HashMap[String, HudiSchemaSpec]()
  val rowTypeMap = new mutable.HashMap[String, RowType]()

  readSchema()

  def readSchema(): Unit = {
    val datasourceConfig = DatasetRegistry.getAllDatasources().filter(f => f.`type`.nonEmpty && f.`type`.equalsIgnoreCase(Constants.DATALAKE_TYPE) && f.status.equalsIgnoreCase("Live"))
    datasourceConfig.map{f =>
      val hudiSchemaSpec = JSONUtil.deserialize[HudiSchemaSpec](f.ingestionSpec)
      val dataset = hudiSchemaSpec.dataset
      hudiSchemaMap.put(dataset, hudiSchemaSpec)
      rowTypeMap.put(dataset, createRowType(hudiSchemaSpec))
    }
  }

  private def createRowType(schema: HudiSchemaSpec): RowType = {
    val columnSpec = schema.schema.columnSpec
    val primaryKey = schema.schema.primaryKey
    val partitionColumn = schema.schema.partitionColumn
    val timeStampColumn = schema.schema.timestampColumn
    val partitionField = schema.schema.columnSpec.filter(f => f.name.equalsIgnoreCase(schema.schema.partitionColumn)).head
    val rowTypeMap = mutable.SortedMap[String, LogicalType]()
    columnSpec.sortBy(_.name).map {
      spec =>
        val isNullable = if (spec.name.matches(s"$primaryKey|$partitionColumn|$timeStampColumn")) false else true
        val columnType = spec.`type` match {
          case "string" => new VarCharType(isNullable, 20)
          case "double" => new DoubleType(isNullable)
          case "long" => new BigIntType(isNullable)
          case "int" => new IntType(isNullable)
          case "boolean" => new BooleanType(true)
          case "map[string, string]" => new MapType(new VarCharType(), new VarCharType())
          case "epoch" => new BigIntType(isNullable)
          case _ => new VarCharType(isNullable, 20)
        }
        rowTypeMap.put(spec.name, columnType)
    }
    if(partitionField.`type`.equalsIgnoreCase("timestamp") || partitionField.`type`.equalsIgnoreCase("epoch")) {
      rowTypeMap.put(partitionField.name + "_partition", new VarCharType(false, 20))
    }
    val rowType: RowType = RowType.of(false, rowTypeMap.values.toArray, rowTypeMap.keySet.toArray)
    logger.info("rowType: " + rowType)
    rowType
  }

  def parseJson(dataset: String, event: String): mutable.Map[String, Any] = {
    val parserSpec = hudiSchemaMap.get(dataset)
    val jsonNode = objectMapper.readTree(event)
    val flattenedEventData = mutable.Map[String, Any]()
    parserSpec.map { spec =>
      val columnSpec = spec.schema.columnSpec
      val partitionField = spec.schema.columnSpec.filter(f => f.name.equalsIgnoreCase(spec.schema.partitionColumn)).head
      spec.inputFormat.flattenSpec.map {
        flattenSpec =>
          flattenSpec.fields.map {
            field =>
              val node = retrieveFieldFromJson(jsonNode, field)
              node.map {
                nodeValue =>
                  try {
                    val fieldDataType = columnSpec.filter(_.name.equalsIgnoreCase(field.name)).head.`type`
                    val fieldValue = fieldDataType match {
                      case "string" => objectMapper.treeToValue(nodeValue, classOf[String])
                      case "int" => objectMapper.treeToValue(nodeValue, classOf[Int])
                      case "long" => objectMapper.treeToValue(nodeValue, classOf[Long])
                      case "double" => objectMapper.treeToValue(nodeValue, classOf[Double])
                      case "epoch" => objectMapper.treeToValue(nodeValue, classOf[Long])
                      case _ => objectMapper.treeToValue(nodeValue, classOf[String])
                    }
                    if(field.name.equalsIgnoreCase(partitionField.name)){
                      if(fieldDataType.equalsIgnoreCase("timestamp")) {
                        flattenedEventData.put(field.name + "_partition", df.format(objectMapper.treeToValue(nodeValue, classOf[Timestamp])))
                      }
                      else if(fieldDataType.equalsIgnoreCase("epoch")) {
                        flattenedEventData.put(field.name + "_partition", df.format(objectMapper.treeToValue(nodeValue, classOf[Long])))
                      }
                    }
                    flattenedEventData.put(field.name, fieldValue)
                  }
                  catch {
                    case ex: Exception =>
                      logger.info("Hudi Schema Parser - Exception: ", ex.getMessage)
                      flattenedEventData.put(field.name, null)
                  }

              }.orElse(flattenedEventData.put(field.name, null))
          }
      }
    }
    logger.info("flattenedEventData: " + flattenedEventData)
    flattenedEventData
  }

  def retrieveFieldFromJson(jsonNode: JsonNode, field: JsonFieldParserSpec): Option[JsonNode] = {
    if (field.`type`.equalsIgnoreCase("path")) {
      field.expr.map{ f => jsonNode.at(s"/${f.split("\\.").tail.mkString("/")}") }
    } else {
      Option(jsonNode.get(field.name))
    }
  }
}
