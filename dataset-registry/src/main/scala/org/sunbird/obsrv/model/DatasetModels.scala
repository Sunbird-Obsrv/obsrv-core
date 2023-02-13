package org.sunbird.obsrv.model

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import org.sunbird.obsrv.core.model.SystemConfig
import org.sunbird.obsrv.model.ValidationMode.ValidationMode

import scala.beans.BeanProperty

object DatasetModels {

  @BeanProperty
  case class ExtractionConfig(@JsonProperty("is_batch_event") isBatchEvent: Option[Boolean] = Some(false),
                              @JsonProperty("extraction_key") extractionKey: Option[String] = Some("events"),
                              @JsonProperty("dedup_config") dedupConfig: Option[DedupConfig])

  case class DedupConfig(@JsonProperty("drop_duplicates") dropDuplicates: Option[Boolean] = Some(false),
                         @JsonProperty("dedup_key") dedupKey: Option[String],
                         @JsonProperty("dedup_period") dedupPeriod: Option[Integer] = Some(SystemConfig.defaultDedupPeriodInSeconds))

  case class ValidationConfig(@JsonProperty("validate") validate: Option[Boolean] = Some(true),
                              @JsonProperty("mode") @JsonScalaEnumeration(classOf[ValidationModeType]) mode: Option[ValidationMode])

  case class DenormFieldConfig(@JsonProperty("denorm_key") denormKey: String, @JsonProperty("redis_db") redisDB: Int,
                               @JsonProperty("denorm_out_field") denormOutField: String)

  case class DenormConfig(@JsonProperty("redis_db_host") redisDBHost: String, @JsonProperty("redis_db_port") redisDBPort: Int,
                          @JsonProperty("denorm_fields") denormFields: List[DenormFieldConfig])

  case class RouterConfig(@JsonProperty("topic") topic: String)

  case class Dataset(@JsonProperty("id") id: String, @JsonProperty("extraction_config") extractionConfig: Option[ExtractionConfig],
                     @JsonProperty("dedup_config") dedupConfig: Option[DedupConfig], @JsonProperty("validation_config") validationConfig: Option[ValidationConfig],
                     @JsonProperty("data_schema") jsonSchema: Option[String], @JsonProperty("denorm_config") denormConfig: Option[DenormConfig],
                     @JsonProperty("router_config") routerConfig: RouterConfig);

  case class Datasource(@JsonProperty("id") id: String, @JsonProperty("dataset_id") datasetId: String,
                        @JsonProperty("ingestion_spec") ingestionSpec: String, @JsonProperty("datasource") datasource: String,
                        @JsonProperty("retention_policy") retentionPolicy: Option[String], @JsonProperty("archival_policy") archivalPolicy: Option[String],
                        @JsonProperty("purge_policy") purgePolicy: Option[String], @JsonProperty("backup_config") backupConfig: Option[String])

  case class DatasetTransformation(@JsonProperty("id") id: String, @JsonProperty("dataset_id") datasetId: String,
                                   @JsonProperty("field_key") fieldKey: String, @JsonProperty("transformation_function") transformationFunction: String,
                                   @JsonProperty("field_out_key") fieldOutKey: String)


}

class ValidationModeType extends TypeReference[ValidationMode.type]
object ValidationMode extends Enumeration {
  type ValidationMode = Value
  val Strict, IgnoreNewFields, DiscardNewFields = Value
}
