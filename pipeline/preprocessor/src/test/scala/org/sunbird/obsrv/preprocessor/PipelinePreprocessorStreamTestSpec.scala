package org.sunbird.obsrv.preprocessor

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.sunbird.obsrv.BaseMetricsReporter
import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.Models.SystemEvent
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil, PostgresConnect}
import org.sunbird.obsrv.preprocessor.fixture.EventFixtures
import org.sunbird.obsrv.preprocessor.task.{PipelinePreprocessorConfig, PipelinePreprocessorStreamTask}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class PipelinePreprocessorStreamTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val pConfig = new PipelinePreprocessorConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(pConfig)
  val customKafkaConsumerProperties: Map[String, String] = Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-event-schema-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )
  implicit val deserializer: StringDeserializer = new StringDeserializer()

  def testConfiguration(): Configuration = {
    val config = new Configuration()
    config.setString("metrics.reporter", "job_metrics_reporter")
    config.setString("metrics.reporter.job_metrics_reporter.class", classOf[BaseMetricsReporter].getName)
    config
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    BaseMetricsReporter.gaugeMetrics.clear()
    EmbeddedKafka.start()(embeddedKafkaConfig)
    prepareTestData()
    createTestTopics()
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.VALID_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.INVALID_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.DUPLICATE_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.MISSING_DATASET_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.INVALID_DATASET_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.INVALID_EVENT_KEY)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.VALID_EVENT_DEDUP_CONFIG_NONE)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.INVALID_EVENT_2)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.EVENT_WITH_ADDL_PROPS_STRICT_MODE)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.EVENT_WITH_ADDL_PROPS_ALLOW_MODE)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.EVENT_WITH_ADDL_PROPS_IGNORE_MODE)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.IGNORED_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.EVENT_WITH_UNKNOWN_VALIDATION_ERR)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.EVENT_WITH_EMPTY_SCHEMA)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.DEDUP_KEY_MISSING)

    flinkCluster.before()
  }

  private def prepareTestData(): Unit = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date) values ('d3', 'dataset', '" + """{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"id":{"type":"string"},"vehicleCode":{"type":"string"},"date":{"type":"string"},"dealer":{"type":"object","properties":{"dealerCode":{"type":"string"},"locationId":{"type":"string"},"email":{"type":"string"},"phone":{"type":"string"}},"additionalProperties":false,"required":["dealerCode","locationId"]},"metrics":{"type":"object","properties":{"bookingsTaken":{"type":"integer"},"deliveriesPromised":{"type":"integer"},"deliveriesDone":{"type":"integer"}},"additionalProperties":false}},"additionalProperties":false,"required":["id","vehicleCode","date"]}""" + "', '{\"validate\": true, \"mode\": \"Strict\"}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Draft', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date) values ('d4', 'dataset', '" + """{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"id":{"type":"string"},"vehicleCode":{"type":"string"},"date":{"type":"string"},"dealer":{"type":"object","properties":{"dealerCode":{"type":"string"},"locationId":{"type":"string"},"email":{"type":"string"},"phone":{"type":"string"}},"additionalProperties":false,"required":["dealerCode","locationId"]},"metrics":{"type":"object","properties":{"bookingsTaken":{"type":"integer"},"deliveriesPromised":{"type":"integer"},"deliveriesDone":{"type":"integer"}},"additionalProperties":false}},"additionalProperties":false,"required":["id","vehicleCode","date"]}""" + "', '{\"validate\": true, \"mode\": \"Strict\"}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date) values ('d5', 'dataset', '" + """{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"id":{"type":"string"},"vehicleCode":{"type":"string"},"date":{"type":"string"},"dealer":{"type":"object","properties":{"dealerCode":{"type":"string"},"locationId":{"type":"string"},"email":{"type":"string"},"phone":{"type":"string"}},"additionalProperties":false,"required":["dealerCode","locationId"]},"metrics":{"type":"object","properties":{"bookingsTaken":{"type":"integer"},"deliveriesPromised":{"type":"integer"},"deliveriesDone":{"type":"integer"}},"additionalProperties":false}},"additionalProperties":false,"required":["id","vehicleCode","date"]}""" + "', '{\"validate\": true, \"mode\": \"IgnoreNewFields\"}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date) values ('d6', 'dataset', '" + """{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"id":{"type":"string","maxLength":5},"vehicleCode":{"type":"string"},"date":{"type":"string"},"dealer":{"type":"object","properties":{"dealerCode":{"type":"string"},"locationId":{"type":"string"},"email":{"type":"string"},"phone":{"type":"string"}},"additionalProperties":false,"required":["dealerCode","locationId"]},"metrics":{"type":"object","properties":{"bookingsTaken":{"type":"integer"},"deliveriesPromised":{"type":"integer"},"deliveriesDone":{"type":"integer"}},"additionalProperties":false}},"additionalProperties":false,"required":["id","vehicleCode","date"]}""" + "', '{\"validate\": true, \"mode\": \"DiscardNewFields\"}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date) values ('d7', 'dataset', '"+EventFixtures.INVALID_SCHEMA+"', '{\"validate\": true, \"mode\": \"Strict\"}','{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, extraction_config, dedup_config, router_config, dataset_config, status, data_version, created_by, updated_by, created_date, updated_date) values ('d8', 'dataset', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"validate\": false, \"mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}}', '{\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}', '{\"topic\":\"d1-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\",\"redis_db_host\":\"localhost\",\"redis_db_port\":"+config.getInt("redis.port")+",\"redis_db\":2}', 'Live', 2, 'System', 'System', now(), now());")
    postgresConnect.closeConnection()
  }

  override def afterAll(): Unit = {
    val redisConnection = new RedisConnect(pConfig.redisHost, pConfig.redisPort, pConfig.redisConnectionTimeout)
    redisConnection.getConnection(config.getInt("redis.database.preprocessor.duplication.store.id")).flushAll()
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      pConfig.kafkaInputTopic, pConfig.kafkaInvalidTopic, pConfig.kafkaSystemTopic, pConfig.kafkaDuplicateTopic, pConfig.kafkaUniqueTopic
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "PipelinePreprocessorStreamTestSpec" should "validate the preprocessor job" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(pConfig)
    val task = new PipelinePreprocessorStreamTask(pConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(pConfig.jobName)
      Thread.sleep(5000)
    }
    val outputEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](pConfig.kafkaUniqueTopic, 5, timeout = 30.seconds)
    val invalidEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](pConfig.kafkaInvalidTopic, 7, timeout = 30.seconds)
    val systemEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](pConfig.kafkaSystemTopic, 8, timeout = 30.seconds)

    validateOutputEvents(outputEvents)
    validateInvalidEvents(invalidEvents)
    validateSystemEvents(systemEvents)

    val mutableMetricsMap = mutable.Map[String, Long]()
    BaseMetricsReporter.gaugeMetrics.toMap.mapValues(f => f.getValue()).map(f => mutableMetricsMap.put(f._1, f._2))
    Console.println("### PipelinePreprocessorStreamTestSpec:metrics ###", JSONUtil.serialize(getPrintableMetrics(mutableMetricsMap)))
    validateMetrics(mutableMetricsMap)

  }

  private def validateOutputEvents(outputEvents: List[String]): Unit = {
    outputEvents.size should be(5)
    outputEvents.foreach(f => println("OutputEvent", f))
    /*
    (OutputEvent,{"event":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"obsrv_meta":{"flags":{"validator":"success","dedup":"success"},"syncts":1701772208183,"prevProcessingTime":1701772214928,"error":{},"processingStartTime":1701772214321,"timespans":{"validator":590,"dedup":17}},"dataset":"d1"})
    (OutputEvent,{"event":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1235","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"obsrv_meta":{"flags":{"validator":"skipped"},"syncts":1701772208476,"prevProcessingTime":1701772215544,"error":{},"processingStartTime":1701772214544,"timespans":{"validator":1000}},"dataset":"d2"})
    (OutputEvent,{"event":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19,"deliveriesRejected":1}},"obsrv_meta":{"flags":{"validator":"success"},"syncts":1701772208577,"prevProcessingTime":1701772215613,"error":{},"processingStartTime":1701772214561,"timespans":{"validator":1052}},"dataset":"d5"})
    (OutputEvent,{"event":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19,"deliveriesRejected":1}},"obsrv_meta":{"flags":{"validator":"success"},"syncts":1701772208597,"prevProcessingTime":1701772215623,"error":{},"processingStartTime":1701772214562,"timespans":{"validator":1061}},"dataset":"d6"})
    (OutputEvent,{"event":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19,"deliveriesRejected":1}},"obsrv_meta":{"flags":{"validator":"skipped"},"syncts":1701772208676,"prevProcessingTime":1701772215637,"error":{},"processingStartTime":1701772214563,"timespans":{"validator":1074}},"dataset":"d7"})
     */
  }

  private def validateInvalidEvents(invalidEvents: List[String]): Unit = {
    invalidEvents.size should be(7)
    /*
    (invalid,{"event":"{\"event\":{\"id\":\"1234\",\"date\":\"2023-03-01\",\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d1\"}","obsrv_meta":{"flags":{"validator":"failed"},"syncts":1701429101820,"prevProcessingTime":1701429108259,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"validator"},"error_code":"ERR_PP_1013","error_msg":"Event failed the schema validation"},"processingStartTime":1701429107624,"timespans":{"validator":635}},"dataset":"d1"})
    (invalid,{"event":"{\"event\":{\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1234\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d1\"}","obsrv_meta":{"flags":{"validator":"success","dedup":"failed"},"syncts":1701429101860,"prevProcessingTime":1701429108501,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"dedup"},"error_code":"ERR_PP_1010","error_msg":"Duplicate event found"},"processingStartTime":1701429107625,"timespans":{"validator":873,"dedup":3}},"dataset":"d1"})
    (invalid,{"event":"{\"event\":{\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1234\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}}}","obsrv_meta":{"flags":{"validator":"failed"},"syncts":1701429101886,"prevProcessingTime":1701429108528,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"validator"},"error_code":"ERR_EXT_1004","error_msg":"Dataset Id is missing from the data"},"processingStartTime":1701429107625,"timespans":{"validator":903}}})
    (invalid,{"event":"{\"event\":{\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1234\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"dX\"}","obsrv_meta":{"flags":{"validator":"failed"},"syncts":1701429101927,"prevProcessingTime":1701429108583,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"validator"},"error_code":"ERR_EXT_1005","error_msg":"Dataset configuration is missing"},"processingStartTime":1701429107626,"timespans":{"validator":957}},"dataset":"dX"})
    (invalid,{"event1":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"event":"{\"event1\":{\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1234\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d2\"}","obsrv_meta":{"flags":{"validator":"failed"},"syncts":1701429101961,"prevProcessingTime":1701429108586,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"validator"},"error_code":"ERR_EXT_1006","error_msg":"Event missing in the batch event"},"processingStartTime":1701429107627,"timespans":{"validator":959}},"dataset":"d2"})
    (invalid,{"event":"{\"event\":{\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":[\"HYUN-CRE-D6\"],\"id\":1234,\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d4\"}","obsrv_meta":{"flags":{"validator":"failed"},"syncts":1701429102063,"prevProcessingTime":1701429108633,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"validator"},"error_code":"ERR_PP_1013","error_msg":"Event failed the schema validation"},"processingStartTime":1701429107631,"timespans":{"validator":1002}},"dataset":"d4"})
    (invalid,{"event":"{\"event\":{\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1234\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19,\"deliveriesRejected\":1}},\"dataset\":\"d4\"}","obsrv_meta":{"flags":{"validator":"failed"},"syncts":1701429102092,"prevProcessingTime":1701429108661,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"validator"},"error_code":"ERR_PP_1013","error_msg":"Event failed the schema validation"},"processingStartTime":1701429107638,"timespans":{"validator":1023}},"dataset":"d4"})
     */
  }

  private def validateSystemEvents(systemEvents: List[String]): Unit = {
    systemEvents.size should be(8)

    systemEvents.foreach(se => {
      val event = JSONUtil.deserialize[SystemEvent](se)
      val error = event.data.error
      if (event.ctx.dataset.getOrElse("ALL").equals("ALL"))
        event.ctx.dataset_type should be(None)
      else if (error.isDefined) {
        val errorCode = error.get.error_code
        if (errorCode.equals(ErrorConstants.MISSING_DATASET_ID.errorCode) ||
          errorCode.equals(ErrorConstants.MISSING_DATASET_CONFIGURATION.errorCode) ||
          errorCode.equals(ErrorConstants.EVENT_MISSING.errorCode)) {
          event.ctx.dataset_type should be(None)
        }
      }
      else
        event.ctx.dataset_type should be(Some("dataset"))
    })
    /*
    (SysEvent:,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"PipelinePreprocessorJob","type":"flink","pid":"validator"},"dataset":"d1", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"validator","pdata_status":"failed","error_type":"RequiredFieldsMissing","error_code":"ERR_PP_1013","error_message":"Event failed the schema validation","error_level":"warn","error_count":1}},"ets":1701428460664})
    (SysEvent:,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"PipelinePreprocessorJob","type":"flink","pid":"validator"},"dataset":"ALL"},"data":{"error":{"pdata_id":"validator","pdata_status":"failed","error_type":"MissingDatasetId","error_code":"ERR_EXT_1004","error_message":"Dataset Id is missing from the data","error_level":"critical","error_count":1},"pipeline_stats":{"validator_status":"failed","validator_time":874}},"ets":1701428460889})
    (SysEvent:,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"PipelinePreprocessorJob","type":"flink","pid":"validator"},"dataset":"dX", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"validator","pdata_status":"failed","error_type":"MissingDatasetId","error_code":"ERR_EXT_1005","error_message":"Dataset configuration is missing","error_level":"critical","error_count":1},"pipeline_stats":{"validator_status":"failed","validator_time":924}},"ets":1701428460927})
    (SysEvent:,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"PipelinePreprocessorJob","type":"flink","pid":"validator"},"dataset":"d2", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"validator","pdata_status":"failed","error_type":"MissingEventData","error_code":"ERR_EXT_1006","error_message":"Event missing in the batch event","error_level":"critical","error_count":1},"pipeline_stats":{"validator_status":"failed","validator_time":925}},"ets":1701428460935})
    (SysEvent:,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"PipelinePreprocessorJob","type":"flink","pid":"validator"},"dataset":"d4", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"validator","pdata_status":"failed","error_type":"DataTypeMismatch","error_code":"ERR_PP_1013","error_message":"Event failed the schema validation","error_level":"warn","error_count":2}},"ets":1701428460987})
    (SysEvent:,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"PipelinePreprocessorJob","type":"flink","pid":"validator"},"dataset":"d4", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"validator","pdata_status":"failed","error_type":"AdditionalFieldsFound","error_code":"ERR_PP_1013","error_message":"Event failed the schema validation","error_level":"warn","error_count":0}},"ets":1701428461010})
    (SysEvent:,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"PipelinePreprocessorJob","type":"flink","pid":"validator"},"dataset":"d6", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"validator","pdata_status":"failed","error_type":"AdditionalFieldsFound","error_code":"ERR_PP_1013","error_message":"Event failed the schema validation","error_level":"warn","error_count":0}},"ets":1701428461064})
     */
  }

  private def validateMetrics(mutableMetricsMap: mutable.Map[String, Long]): Unit = {
    mutableMetricsMap(s"${pConfig.jobName}.ALL.${pConfig.eventFailedMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.dX.${pConfig.eventFailedMetricsCount}") should be(1)

    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.validationFailureMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.duplicationProcessedEventMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.duplicationEventMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.validationSuccessMetricsCount}") should be(2)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.validationTotalMetricsCount}") should be(3)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.duplicationTotalMetricsCount}") should be(2)

    mutableMetricsMap(s"${pConfig.jobName}.d2.${pConfig.duplicationSkippedEventMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d2.${pConfig.validationSkipMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d2.${pConfig.eventFailedMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d2.${pConfig.validationTotalMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d2.${pConfig.duplicationTotalMetricsCount}") should be(1)

    mutableMetricsMap(s"${pConfig.jobName}.d3.${pConfig.validationTotalMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d3.${pConfig.eventIgnoredMetricsCount}") should be(1)

    mutableMetricsMap(s"${pConfig.jobName}.d4.${pConfig.validationTotalMetricsCount}") should be(2)
    mutableMetricsMap(s"${pConfig.jobName}.d4.${pConfig.validationFailureMetricsCount}") should be(2)

    mutableMetricsMap(s"${pConfig.jobName}.d5.${pConfig.validationTotalMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d5.${pConfig.validationSuccessMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d5.${pConfig.duplicationTotalMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d5.${pConfig.duplicationSkippedEventMetricsCount}") should be(1)

    mutableMetricsMap(s"${pConfig.jobName}.d6.${pConfig.validationTotalMetricsCount}") should be(2)
    mutableMetricsMap(s"${pConfig.jobName}.d6.${pConfig.validationSuccessMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d6.${pConfig.validationFailureMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d6.${pConfig.duplicationTotalMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d6.${pConfig.duplicationSkippedEventMetricsCount}") should be(1)

    mutableMetricsMap(s"${pConfig.jobName}.d8.${pConfig.validationTotalMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d8.${pConfig.validationSkipMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d8.${pConfig.duplicationTotalMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d8.${pConfig.duplicationProcessedEventMetricsCount}") should be(1)
  }

}