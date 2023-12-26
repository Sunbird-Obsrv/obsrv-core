package org.sunbird.obsrv.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import org.sunbird.obsrv.core.model.SystemConfig
import org.sunbird.obsrv.model.DatasetStatus.DatasetStatus
import org.sunbird.obsrv.model.TransformMode.TransformMode
import org.sunbird.obsrv.model.ValidationMode.ValidationMode

import java.sql.Timestamp
import scala.beans.BeanProperty

object DatasetModels {

  @BeanProperty
  case class ExtractionConfig(@JsonProperty("is_batch_event") isBatchEvent: Option[Boolean] = Some(false),
                              @JsonProperty("extraction_key") extractionKey: Option[String] = Some("events"),
                              @JsonProperty("dedup_config") dedupConfig: Option[DedupConfig])

  case class DedupConfig(@JsonProperty("drop_duplicates") dropDuplicates: Option[Boolean] = Some(false),
                         @JsonProperty("dedup_key") dedupKey: Option[String],
                         @JsonProperty("dedup_period") dedupPeriod: Option[Integer] = Some(SystemConfig.getInt("defaultDedupPeriodInSeconds", 604800)))

  case class ValidationConfig(@JsonProperty("validate") validate: Option[Boolean] = Some(true),
                              @JsonProperty("mode") @JsonScalaEnumeration(classOf[ValidationModeType]) mode: Option[ValidationMode])

  case class DenormFieldConfig(@JsonProperty("denorm_key") denormKey: String, @JsonProperty("redis_db") redisDB: Int,
                               @JsonProperty("denorm_out_field") denormOutField: String)

  case class DenormConfig(@JsonProperty("redis_db_host") redisDBHost: String, @JsonProperty("redis_db_port") redisDBPort: Int,
                          @JsonProperty("denorm_fields") denormFields: List[DenormFieldConfig])

  case class RouterConfig(@JsonProperty("topic") topic: String)

  case class DatasetConfig(@JsonProperty("data_key") key: String, @JsonProperty("timestamp_key") tsKey: String, @JsonProperty("entry_topic") entryTopic: String,
                           @JsonProperty("exclude_fields") excludeFields: Option[List[String]] = None, @JsonProperty("redis_db_host") redisDBHost: Option[String] = None,
                           @JsonProperty("redis_db_port") redisDBPort: Option[Int] = None, @JsonProperty("redis_db") redisDB: Option[Int] = None,
                           @JsonProperty("index_data") indexData: Option[Boolean] = None, @JsonProperty("timestamp_format") tsFormat: Option[String] = None,
                           @JsonProperty("dataset_tz") datasetTimezone: Option[String] = None)

  case class Dataset(@JsonProperty("id") id: String, @JsonProperty("type") datasetType: String, @JsonProperty("extraction_config") extractionConfig: Option[ExtractionConfig],
                     @JsonProperty("dedup_config") dedupConfig: Option[DedupConfig], @JsonProperty("validation_config") validationConfig: Option[ValidationConfig],
                     @JsonProperty("data_schema") jsonSchema: Option[String], @JsonProperty("denorm_config") denormConfig: Option[DenormConfig],
                     @JsonProperty("router_config") routerConfig: RouterConfig, datasetConfig: DatasetConfig, @JsonProperty("status") @JsonScalaEnumeration(classOf[DatasetStatusType]) status: DatasetStatus,
                     @JsonProperty("tags") tags: Option[Array[String]] = None, @JsonProperty("data_version") dataVersion: Option[Int] = None)

  case class Condition(@JsonProperty("type") `type`: String, @JsonProperty("expr") expr: String)

  case class TransformationFunction(@JsonProperty("type") `type`: String, @JsonProperty("condition") condition: Option[Condition], @JsonProperty("expr") expr: String)

  case class DatasetTransformation(@JsonProperty("id") id: String, @JsonProperty("dataset_id") datasetId: String,
                                   @JsonProperty("field_key") fieldKey: String, @JsonProperty("transformation_function") transformationFunction: TransformationFunction,
                                   @JsonProperty("status") status: String, @JsonProperty("mode") @JsonScalaEnumeration(classOf[TransformModeType]) mode: Option[TransformMode] = Some(TransformMode.Strict))

  case class ConnectorConfig(@JsonProperty("kafkaBrokers") kafkaBrokers: String, @JsonProperty("topic") topic: String, @JsonProperty("type") databaseType: String,
                             @JsonProperty("connection") connection: Connection, @JsonProperty("tableName") tableName: String, @JsonProperty("databaseName") databaseName: String,
                             @JsonProperty("pollingInterval") pollingInterval: PollingInterval, @JsonProperty("authenticationMechanism") authenticationMechanism: AuthenticationMechanism,
                             @JsonProperty("batchSize") batchSize: Int, @JsonProperty("timestampColumn") timestampColumn: String)

  case class Connection(@JsonProperty("host") host: String, @JsonProperty("port") port: String)

  case class PollingInterval(@JsonProperty("type") pollingType: String, @JsonProperty("cronExpression") cronExpression: String)

  case class AuthenticationMechanism(@JsonProperty("encrypted") encrypted: Boolean, @JsonProperty("encryptedValues") encryptedValues: String)

  case class ConnectorStats(@JsonProperty("last_fetch_timestamp") lastFetchTimestamp: Timestamp, @JsonProperty("records") records: Long, @JsonProperty("avg_batch_read_time") avgBatchReadTime: Long, @JsonProperty("disconnections") disconnections: Int)

  case class DatasetSourceConfig(@JsonProperty("id") id: String, @JsonProperty("dataset_id") datasetId: String,
                                 @JsonProperty("connector_type") connectorType: String, @JsonProperty("connector_config") connectorConfig: ConnectorConfig,
                                 @JsonProperty("status") status: String, @JsonProperty("connector_stats") connectorStats: Option[ConnectorStats] = None)

  case class DataSource(@JsonProperty("id") id: String, @JsonProperty("datasource") datasource: String, @JsonProperty("dataset_id") datasetId: String,
                        @JsonProperty("ingestion_spec") ingestionSpec: String, @JsonProperty("datasource_ref") datasourceRef: String)

}

class ValidationModeType extends TypeReference[ValidationMode.type]

object ValidationMode extends Enumeration {
  type ValidationMode = Value
  val Strict, IgnoreNewFields, DiscardNewFields = Value
}

class TransformModeType extends TypeReference[TransformMode.type]

object TransformMode extends Enumeration {
  type TransformMode = Value
  val Strict, Lenient = Value
}

class DatasetStatusType extends TypeReference[DatasetStatus.type]

object DatasetStatus extends Enumeration {
  type DatasetStatus = Value
  val Draft, Publish, Live, Retired, Purged = Value
}