package org.sunbird.obsrv.core.model

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import org.sunbird.obsrv.core.model.ErrorLevel.ErrorLevel
import org.sunbird.obsrv.core.model.EventID.EventID
import org.sunbird.obsrv.core.model.FunctionalError.FunctionalError
import org.sunbird.obsrv.core.model.ModuleID.ModuleID
import org.sunbird.obsrv.core.model.PDataType.PDataType
import org.sunbird.obsrv.core.model.Producer.Producer
import org.sunbird.obsrv.core.model.StatusCode.StatusCode
import com.fasterxml.jackson.annotation.JsonProperty
import org.sunbird.obsrv.core.exception.ObsrvException

object Models {

  case class PData(id: String,@JsonScalaEnumeration(classOf[PDataTypeType]) `type`: PDataType,@JsonScalaEnumeration(classOf[ProducerType]) pid: Option[Producer])

  case class ContextData(@JsonScalaEnumeration(classOf[ModuleIDType]) module: ModuleID, pdata: PData, dataset: Option[String] = None, dataset_type: Option[String] = None, eid: Option[String] = None)

  case class ErrorLog(@JsonScalaEnumeration(classOf[ProducerType]) pdata_id: Producer, @JsonScalaEnumeration(classOf[StatusCodeType]) pdata_status: StatusCode, @JsonScalaEnumeration(classOf[FunctionalErrorType]) error_type: FunctionalError, error_code: String, error_message: String,@JsonScalaEnumeration(classOf[ErrorLevelType]) error_level: ErrorLevel, error_count:Option[Int] = None)

  case class PipelineStats(extractor_events: Option[Int] = None, @JsonScalaEnumeration(classOf[StatusCodeType]) extractor_status: Option[StatusCode] = None,
                           extractor_time: Option[Long] = None, @JsonScalaEnumeration(classOf[StatusCodeType]) validator_status: Option[StatusCode] = None, validator_time: Option[Long] = None,
                           @JsonScalaEnumeration(classOf[StatusCodeType]) dedup_status: Option[StatusCode] = None, dedup_time: Option[Long] = None, @JsonScalaEnumeration(classOf[StatusCodeType]) denorm_status: Option[StatusCode] = None,
                           denorm_time: Option[Long] = None, @JsonScalaEnumeration(classOf[StatusCodeType]) transform_status: Option[StatusCode] = None, transform_time: Option[Long] = None,
                           total_processing_time: Option[Long] = None, latency_time: Option[Long] = None, processing_time: Option[Long] = None)

  case class EData(error: Option[ErrorLog] = None, pipeline_stats: Option[PipelineStats] = None, extra: Option[Map[String, AnyRef]] = None)

  case class SystemEvent(@JsonScalaEnumeration(classOf[EventIDType]) etype: EventID, ctx: ContextData, data: EData, ets: Long = System.currentTimeMillis())
  case class SystemSetting(key: String, value: String, category: String, valueType: String, label: Option[String])

}

class EventIDType extends TypeReference[EventID.type]
object EventID extends Enumeration {
  type EventID = Value
  val LOG, METRIC = Value
}

class ErrorLevelType extends TypeReference[ErrorLevel.type]
object ErrorLevel extends Enumeration {
  type ErrorLevel = Value
  val debug, info, warn, critical = Value
}

class FunctionalErrorType extends TypeReference[FunctionalError.type]
object FunctionalError extends Enumeration {
  type FunctionalError = Value
  val DedupFailed, RequiredFieldsMissing, DataTypeMismatch, AdditionalFieldsFound, UnknownValidationError, MissingDatasetId, MissingEventData, MissingTimestampKey,
  EventSizeExceeded, ExtractionDataFormatInvalid, DenormKeyMissing, DenormKeyInvalid, DenormDataNotFound, InvalidJsonData,
  TransformParseError, TransformEvalError, TransformFailedError, MissingMasterDatasetKey, TransformFieldMissing = Value
}

class ProducerType extends TypeReference[Producer.type]
object Producer extends Enumeration {
  type Producer = Value
  val extractor, dedup, validator, denorm, transformer, router, masterdataprocessor = Value
}

class ModuleIDType extends TypeReference[ModuleID.type]
object ModuleID extends Enumeration {
  type ModuleID = Value
  val ingestion, processing, storage, query = Value
}

class StatusCodeType extends TypeReference[StatusCode.type]
object StatusCode extends Enumeration {
  type StatusCode = Value
  val success, failed, skipped, partial = Value
}

class PDataTypeType extends TypeReference[PDataType.type]
object PDataType extends Enumeration {
  type PDataType = Value
  val flink, spark, druid, kafka, api = Value
}

object Stats extends Enumeration {
  type Stats = Value
  val total_processing_time, latency_time, processing_time = Value
}
