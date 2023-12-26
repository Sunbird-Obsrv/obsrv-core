package org.sunbird.obsrv.core.model

object ErrorConstants extends Enumeration {

  type Error = ErrorValue
  case class ErrorValue(errorCode: String, errorMsg: String)
  protected final def ErrorInternalValue(errorCode: String, errorMsg: String): ErrorValue = {
    ErrorValue(errorCode, errorMsg)
  }

  val NO_IMPLEMENTATION_FOUND = ErrorInternalValue("ERR_0001", "Unimplemented method")
  val NO_EXTRACTION_DATA_FOUND = ErrorInternalValue("ERR_EXT_1001", "Unable to extract the data from the extraction key")
  val EXTRACTED_DATA_NOT_A_LIST = ErrorInternalValue("ERR_EXT_1002", "The extracted data is not a list")
  val EVENT_SIZE_EXCEEDED = ErrorInternalValue("ERR_EXT_1003", "Event size has exceeded max configured size")
  val MISSING_DATASET_ID = ErrorInternalValue("ERR_EXT_1004", "Dataset Id is missing from the data")
  val MISSING_DATASET_CONFIGURATION = ErrorInternalValue("ERR_EXT_1005", "Dataset configuration is missing")
  val EVENT_MISSING = ErrorInternalValue("ERR_EXT_1006", "Event missing in the batch event")
  val NO_DEDUP_KEY_FOUND = ErrorInternalValue("ERR_DEDUP_1007", "No dedup key found or missing data")
  val DEDUP_KEY_NOT_A_STRING_OR_NUMBER = ErrorInternalValue("ERR_DEDUP_1008", "Dedup key value is not a String or Text")
  val DUPLICATE_BATCH_EVENT_FOUND = ErrorInternalValue("ERR_EXT_1009", "Duplicate batch event found")
  val DUPLICATE_EVENT_FOUND = ErrorInternalValue("ERR_PP_1010", "Duplicate event found")
  val JSON_SCHEMA_NOT_FOUND = ErrorInternalValue("ERR_PP_1011", "Json schema not found for the dataset")
  val INVALID_JSON_SCHEMA = ErrorInternalValue("ERR_PP_1012", "Invalid json schema")
  val SCHEMA_VALIDATION_FAILED = ErrorInternalValue("ERR_PP_1013", "Event failed the schema validation")
  val DENORM_KEY_MISSING = ErrorInternalValue("ERR_DENORM_1014", "No denorm key found or missing data for the specified key")
  val DENORM_KEY_NOT_A_STRING_OR_NUMBER = ErrorInternalValue("ERR_DENORM_1015", "Denorm key value is not a String or Number")
  val DENORM_DATA_NOT_FOUND = ErrorInternalValue("ERR_DENORM_1016", "Denorm data not found for the given key")
  val MISSING_DATASET_CONFIG_KEY = ErrorInternalValue("ERR_MASTER_DATA_1017", "Master dataset configuration key is missing")
  val ERR_INVALID_EVENT =  ErrorInternalValue("ERR_EXT_1018", "Invalid JSON event, error while deserializing the event")
  val INDEX_KEY_MISSING_OR_BLANK = ErrorInternalValue("ERR_ROUTER_1019", "Unable to index data as the timestamp key is missing or blank or not a datetime value")
  val INVALID_EXPR_FUNCTION = ErrorInternalValue("ERR_TRANSFORM_1020", "Transformation expression function is not valid")
  val ERR_EVAL_EXPR_FUNCTION = ErrorInternalValue("ERR_TRANSFORM_1021", "Unable to evaluate the transformation expression function")
  val ERR_UNKNOWN_TRANSFORM_EXCEPTION = ErrorInternalValue("ERR_TRANSFORM_1022", "Unable to evaluate the transformation expression function")
  val ERR_TRANSFORMATION_FAILED = ErrorInternalValue("ERR_TRANSFORM_1023", "Atleast one mandatory transformation has failed")
  val TRANSFORMATION_FIELD_MISSING = ErrorInternalValue("ERR_TRANSFORM_1024", "Transformation field is either missing or blank")
  val SYSTEM_SETTING_INVALID_TYPE = ErrorInternalValue("ERR_SYSTEM_SETTING_1025", "Invalid value type for system setting")
  val SYSTEM_SETTING_NOT_FOUND = ErrorInternalValue("ERR_SYSTEM_SETTING_1026", "System setting not found for requested key")
  val SYSTEM_SETTING_DEFAULT_VALUE_NOT_FOUND = ErrorInternalValue("ERR_SYSTEM_SETTING_1027", "Default value not found for requested key")

}
