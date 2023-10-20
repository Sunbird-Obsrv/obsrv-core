package org.sunbird.obsrv.core.model

object ErrorConstants extends Enumeration {

  type Error = ErrorValue
  case class ErrorValue(errorCode: String, errorMsg: String)
  protected final def ErrorInternalValue(errorCode: String, errorMsg: String): ErrorValue = {
    ErrorValue(errorCode, errorMsg)
  }

  val NO_IMPLEMENTATION_FOUND = ErrorInternalValue("ERR_0001", "Unimplemented method")

  val NO_EXTRACTION_DATA_FOUND = ErrorInternalValue("ERR_EXT_1001", "Unable to extract the data from the extraction key")
  val EXTRACTED_DATA_NOT_A_LIST = ErrorInternalValue("ERR_EXT_1002", "The extracted data is not an list")
  val EVENT_SIZE_EXCEEDED = ErrorInternalValue("ERR_EXT_1003", ("Event size has exceeded max configured size of " + SystemConfig.maxEventSize))
  val EVENT_MISSING = ErrorInternalValue("ERR_EXT_1006", "Event missing in the batch event")
  val MISSING_DATASET_ID = ErrorInternalValue("ERR_EXT_1004", "Dataset Id is missing from the data")
  val MISSING_DATASET_CONFIGURATION = ErrorInternalValue("ERR_EXT_1005", "Dataset configuration is missing")

  val NO_DEDUP_KEY_FOUND = ErrorInternalValue("ERR_DEDUP_1007", "No dedup key found or missing data")
  val DEDUP_KEY_NOT_A_STRING = ErrorInternalValue("ERR_DEDUP_1008", "Dedup key value is not a String or Text")
  val DUPLICATE_BATCH_EVENT_FOUND = ErrorInternalValue("ERR_EXT_1009", "Duplicate batch event found")

  val DUPLICATE_EVENT_FOUND = ErrorInternalValue("ERR_PP_1010", "Duplicate event found")
  val JSON_SCHEMA_NOT_FOUND = ErrorInternalValue("ERR_PP_1011", "Json schema not found for the dataset")
  val INVALID_JSON_SCHEMA = ErrorInternalValue("ERR_PP_1012", "Invalid json schema")
  val SCHEMA_VALIDATION_FAILED = ErrorInternalValue("ERR_PP_1013", "Event failed the schema validation")

  val DENORM_KEY_MISSING = ErrorInternalValue("ERR_DENORM_1014", "No denorm key found or missing data for the specified key")
  val DENORM_KEY_NOT_A_STRING = ErrorInternalValue("ERR_DENORM_1015", "Denorm key value is not a String or Text")
  val MISSING_DATASET_CONFIG_KEY = ErrorInternalValue("ERR_EXT_1016", "Dataset configuration key is missing")
}
