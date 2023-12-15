package org.sunbird.spec

import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.obsrv.core.model.FunctionalError.FunctionalError
import org.sunbird.obsrv.core.model.Models._
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.util.{DatasetKeySelector, JSONUtil}

import scala.collection.mutable

case class FuncErrorList(@JsonScalaEnumeration(classOf[FunctionalErrorType]) list: List[FunctionalError])
class ModelsTestSpec extends FlatSpec with Matchers {

  "ModelsTestSpec" should "cover all error constants" in {

    ErrorConstants.NO_IMPLEMENTATION_FOUND.errorCode should be("ERR_0001")
    ErrorConstants.NO_EXTRACTION_DATA_FOUND.errorCode should be("ERR_EXT_1001")
    ErrorConstants.EXTRACTED_DATA_NOT_A_LIST.errorCode should be("ERR_EXT_1002")
    ErrorConstants.EVENT_SIZE_EXCEEDED.errorCode should be("ERR_EXT_1003")
    ErrorConstants.EVENT_MISSING.errorCode should be("ERR_EXT_1006")
    ErrorConstants.MISSING_DATASET_ID.errorCode should be("ERR_EXT_1004")
    ErrorConstants.MISSING_DATASET_CONFIGURATION.errorCode should be("ERR_EXT_1005")
    ErrorConstants.NO_DEDUP_KEY_FOUND.errorCode should be("ERR_DEDUP_1007")
    ErrorConstants.DEDUP_KEY_NOT_A_STRING_OR_NUMBER.errorCode should be("ERR_DEDUP_1008")
    ErrorConstants.DUPLICATE_BATCH_EVENT_FOUND.errorCode should be("ERR_EXT_1009")
    ErrorConstants.DUPLICATE_EVENT_FOUND.errorCode should be("ERR_PP_1010")
    ErrorConstants.JSON_SCHEMA_NOT_FOUND.errorCode should be("ERR_PP_1011")
    ErrorConstants.INVALID_JSON_SCHEMA.errorCode should be("ERR_PP_1012")
    ErrorConstants.SCHEMA_VALIDATION_FAILED.errorCode should be("ERR_PP_1013")
    ErrorConstants.DENORM_KEY_MISSING.errorCode should be("ERR_DENORM_1014")
    ErrorConstants.DENORM_KEY_NOT_A_STRING_OR_NUMBER.errorCode should be("ERR_DENORM_1015")
    ErrorConstants.DENORM_DATA_NOT_FOUND.errorCode should be("ERR_DENORM_1016")
    ErrorConstants.MISSING_DATASET_CONFIG_KEY.errorCode should be("ERR_MASTER_DATA_1017")
    ErrorConstants.ERR_INVALID_EVENT.errorCode should be("ERR_EXT_1018")
    ErrorConstants.INDEX_KEY_MISSING_OR_BLANK.errorCode should be("ERR_ROUTER_1019")
    ErrorConstants.INVALID_EXPR_FUNCTION.errorCode should be("ERR_TRANSFORM_1020")
    ErrorConstants.ERR_EVAL_EXPR_FUNCTION.errorCode should be("ERR_TRANSFORM_1021")
    ErrorConstants.ERR_UNKNOWN_TRANSFORM_EXCEPTION.errorCode should be("ERR_TRANSFORM_1022")
    ErrorConstants.ERR_TRANSFORMATION_FAILED.errorCode should be("ERR_TRANSFORM_1023")
  }

  it should "cover system event model" in {

    Stats.withName("latency_time") should be (Stats.latency_time)
    Stats.withName("processing_time") should be (Stats.processing_time)
    Stats.withName("total_processing_time") should be (Stats.total_processing_time)

    PDataType.withName("flink") should be(PDataType.flink)
    PDataType.withName("api") should be(PDataType.api)
    PDataType.withName("kafka") should be(PDataType.kafka)
    PDataType.withName("druid") should be(PDataType.druid)
    PDataType.withName("spark") should be(PDataType.spark)

    StatusCode.withName("failed") should be (StatusCode.failed)
    StatusCode.withName("partial") should be (StatusCode.partial)
    StatusCode.withName("skipped") should be (StatusCode.skipped)
    StatusCode.withName("success") should be (StatusCode.success)

    ModuleID.withName("ingestion") should be(ModuleID.ingestion)
    ModuleID.withName("processing") should be(ModuleID.processing)
    ModuleID.withName("storage") should be(ModuleID.storage)
    ModuleID.withName("query") should be(ModuleID.query)

    ModuleID.withName("ingestion") should be(ModuleID.ingestion)
    ModuleID.withName("processing") should be(ModuleID.processing)
    ModuleID.withName("storage") should be(ModuleID.storage)
    ModuleID.withName("query") should be(ModuleID.query)

    Producer.withName("extractor") should be(Producer.extractor)
    Producer.withName("validator") should be(Producer.validator)
    Producer.withName("dedup") should be(Producer.dedup)
    Producer.withName("denorm") should be(Producer.denorm)
    Producer.withName("transformer") should be(Producer.transformer)
    Producer.withName("router") should be(Producer.router)
    Producer.withName("masterdataprocessor") should be(Producer.masterdataprocessor)

    EventID.withName("METRIC") should be (EventID.METRIC)
    EventID.withName("LOG") should be (EventID.LOG)

    ErrorLevel.withName("info") should be (ErrorLevel.info)
    ErrorLevel.withName("warn") should be (ErrorLevel.warn)
    ErrorLevel.withName("debug") should be (ErrorLevel.debug)
    ErrorLevel.withName("critical") should be (ErrorLevel.critical)

    val funcErrorsStringList = FunctionalError.values.map(f => f.toString).toList
    val funcErrors = JSONUtil.deserialize[FuncErrorList](JSONUtil.serialize(Map("list" -> funcErrorsStringList)))
    funcErrors.list.contains(FunctionalError.MissingTimestampKey) should be (true)

    val sysEvent = SystemEvent(etype = EventID.METRIC,
      ctx = ContextData(module = ModuleID.processing, pdata = PData(id = "testjob", `type` = PDataType.flink, pid = Some(Producer.router)), dataset = Some("d1"), eid = Some("event1")),
      data = EData(
        error = Some(ErrorLog(pdata_id = Producer.router, pdata_status = StatusCode.failed, error_type = FunctionalError.MissingTimestampKey, error_code = ErrorConstants.DENORM_KEY_MISSING.errorCode, error_message = ErrorConstants.DENORM_KEY_MISSING.errorMsg, error_level = ErrorLevel.warn, error_count = Some(1))),
        pipeline_stats = Some(PipelineStats(extractor_events = Some(2), extractor_status = Some(StatusCode.success), extractor_time = Some(123l), validator_status = Some(StatusCode.success), validator_time = Some(786l), dedup_status = Some(StatusCode.skipped), dedup_time = Some(0l), denorm_status = Some(StatusCode.partial), denorm_time = Some(345l), transform_status = Some(StatusCode.success), transform_time = Some(98l), total_processing_time = Some(1543l), latency_time = Some(23l), processing_time = Some(1520l))), Some(Map("duration" -> 2000.asInstanceOf[AnyRef]))
      )
    )
    sysEvent.etype should be (EventID.METRIC)

    val config: Config = ConfigFactory.load("test2.conf")
    val bsMapConfig = new BaseProcessTestMapConfig(config)
    bsMapConfig.kafkaProducerProperties.get(ProducerConfig.COMPRESSION_TYPE_CONFIG).asInstanceOf[String] should be ("snappy")
    bsMapConfig.kafkaConsumerProperties()
    bsMapConfig.enableDistributedCheckpointing should be (None)
    bsMapConfig.checkpointingBaseUrl should be (None)
    bsMapConfig.datasetType() should be ("master-dataset")

    val dsk = new DatasetKeySelector()
    dsk.getKey(mutable.Map("dataset" -> "d1".asInstanceOf[AnyRef])) should be ("d1")

    JSONUtil.getJsonType("""{"test":123}""") should be ("OBJECT")
    JSONUtil.getJsonType("""{"test":123""") should be ("NOT_A_JSON")
    JSONUtil.getJsonType("""123""") should be ("NOT_A_JSON")

  }

}
