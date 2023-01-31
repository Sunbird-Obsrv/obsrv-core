package org.sunbird.obsrv.model

import ValidationModes.ValidationMode
import org.sunbird.obsrv.core.model.SystemConfig

object DatasetModels {
  case class ExtractionConfig(isBatchEvent: Option[Boolean] = Some(false), extractionKey: Option[String] = Some("events"), dedupConfig: Option[DedupConfig])

  case class DedupConfig(dropDuplicates: Option[Boolean] = Some(false), dedupKey: Option[String], dedupPeriod: Option[Integer] = Some(SystemConfig.defaultDedupPeriodInSeconds))

  case class ValidationConfig(validate: Option[Boolean] = Some(true), mode: Option[ValidationMode])

  case class DenormFieldConfig(denormKey: String, redisDB: Int, denormOutField: String)

  case class DenormConfig(redisDBHost: String, redisDBPort: Int, denormFields: List[DenormFieldConfig])

  case class Dataset(id: String, schema: String, extractionConfig: Option[ExtractionConfig], dedupConfig: Option[DedupConfig],
                     validationConfig: Option[ValidationConfig], jsonSchema: Option[String], denormConfig: Option[DenormConfig]);

}

object ValidationModes extends Enumeration {
  type ValidationMode = Value
  val Strict, IgnoreNewFields, DiscardNewFields = Value
}
