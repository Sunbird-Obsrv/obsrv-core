package org.sunbird.obsrv.model

import ValidationModes.ValidationMode
import org.sunbird.obsrv.core.model.SystemConfig

object DatasetModels {
  case class ExtractionConfig(val isBatchEvent: Option[Boolean]=Some(false), val extractionKey: Option[String] = Some("events"), val dedupConfig: Option[DedupConfig])

  case class DedupConfig(val dropDuplicates: Option[Boolean]=Some(false), val dedupKey: Option[String], val dedupPeriod: Option[Integer] = Some(SystemConfig.defaultDedupPeriodInSeconds))

  case class ValidationConfig(val validate: Option[Boolean]=Some(true), val mode: Option[ValidationMode])

  case class Dataset(val id: String, val schema: String, val extractionConfig: Option[ExtractionConfig], val dedupConfig: Option[DedupConfig], val validationConfig: Option[ValidationConfig], val jsonSchema: Option[String]);

}

object ValidationModes extends Enumeration {
  type ValidationMode = Value
  val Strict, IgnoreNewFields, DiscardNewFields = Value
}
