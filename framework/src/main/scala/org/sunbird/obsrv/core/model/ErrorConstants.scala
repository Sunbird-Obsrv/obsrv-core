package org.sunbird.obsrv.core.model

object ErrorConstants extends Enumeration {

  type Error = ErrorValue
  case class ErrorValue(errorCode: String, errorMsg: String)
  protected final def ErrorInternalValue(errorCode: String, errorMsg: String): ErrorValue = {
    ErrorValue(errorCode, errorMsg)
  }

  val NO_EXTRACTION_DATA_FOUND = ErrorInternalValue("ERR_EXT_1001", "Unable to extract the data from the extraction key")
  val EXTRACTED_DATA_NOT_A_LIST = ErrorInternalValue("ERR_EXT_1002", "The extracted data is not an list")
  val EVENT_SIZE_EXCEEDED = ErrorInternalValue("ERR_EXT_1003", ("Event size has exceeded max configured size of " + SystemConfig.maxEventSize))
  val MISSING_DATASET_ID = ErrorInternalValue("ERR_EXT_1004", "Dataset Id is missing from the data")

  val NO_DEDUP_KEY_FOUND = ErrorInternalValue("ERR_DEDUP_1005", "No dedup key found or missing data")
  val DEDUP_KEY_NOT_A_STRING = ErrorInternalValue("ERR_DEDUP_1006", "Dedup key value is not a String or Text")
  val DUPLICATE_BATCH_EVENT_FOUND = ErrorInternalValue("ERR_EXT_1007", "Duplicate batch event found")
}
