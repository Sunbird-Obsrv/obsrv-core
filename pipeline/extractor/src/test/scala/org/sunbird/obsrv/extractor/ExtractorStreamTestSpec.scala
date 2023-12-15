package org.sunbird.obsrv.extractor

import com.typesafe.config.{Config, ConfigFactory}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.sunbird.obsrv.BaseMetricsReporter
import org.sunbird.obsrv.core.cache.RedisConnect
import org.sunbird.obsrv.core.model.Models.SystemEvent
import org.sunbird.obsrv.core.model.SystemConfig
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil}
import org.sunbird.obsrv.extractor.task.{ExtractorConfig, ExtractorStreamTask}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class ExtractorStreamTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val pConfig = new ExtractorConfig(config)
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
    createTestTopics()
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixture.INVALID_JSON)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixture.MISSING_DEDUP_KEY)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixture.LARGE_JSON_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixture.LARGE_JSON_BATCH)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixture.VALID_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixture.VALID_BATCH)

    flinkCluster.before()
  }

  override def afterAll(): Unit = {
    val redisConnection = new RedisConnect(pConfig.redisHost, pConfig.redisPort, pConfig.redisConnectionTimeout)
    redisConnection.getConnection(config.getInt("redis.database.extractor.duplication.store.id")).flushAll()
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      pConfig.kafkaInputTopic, pConfig.kafkaFailedTopic, pConfig.kafkaSystemTopic, pConfig.kafkaDuplicateTopic, pConfig.kafkaBatchFailedTopic
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "ExtractorStreamTestSpec" should "validate the negative scenarios in extractor job" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(pConfig)
    val task = new ExtractorStreamTask(pConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(pConfig.jobName)
    }
    val batchFailedEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](pConfig.kafkaBatchFailedTopic, 1, timeout = 30.seconds)
    val invalidEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](pConfig.kafkaFailedTopic, 2, timeout = 30.seconds)
    val systemEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](pConfig.kafkaSystemTopic, 6, timeout = 30.seconds)
    val outputEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](pConfig.kafkaSuccessTopic, 3, timeout = 30.seconds)

    validateOutputEvents(outputEvents)
    validateBatchFailedEvents(batchFailedEvents)
    validateInvalidEvents(invalidEvents)
    validateSystemEvents(systemEvents)

    val mutableMetricsMap = mutable.Map[String, Long]()
    BaseMetricsReporter.gaugeMetrics.toMap.mapValues(f => f.getValue()).map(f => mutableMetricsMap.put(f._1, f._2))
    Console.println("### ExtractorStreamTestSpec:metrics ###", JSONUtil.serialize(getPrintableMetrics(mutableMetricsMap)))
    validateMetrics(mutableMetricsMap)

    val config2: Config = ConfigFactory.load("test2.conf")
    val extractorConfig = new ExtractorConfig(config2)
    extractorConfig.eventMaxSize should be (SystemConfig.maxEventSize)
  }

  private def validateOutputEvents(outputEvents: List[String]) = {
    outputEvents.size should be (3)
    //TODO: Add assertions for all 3 events
    /*
    (OutEvent,{"event":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"obsrv_meta":{"flags":{"extractor":"success"},"syncts":1701760331686,"prevProcessingTime":1701760337492,"error":{},"processingStartTime":1701760337087,"timespans":{"extractor":405}},"dataset":"d1"})
    (OutEvent,{"event":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"3","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"obsrv_meta":{"flags":{"extractor":"skipped"},"syncts":1701760331771,"prevProcessingTime":1701760337761,"error":{},"processingStartTime":1701760337089,"timespans":{"extractor":672}},"dataset":"d1"})
    (OutEvent,{"event":{"dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"4","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"obsrv_meta":{"flags":{"extractor":"success"},"syncts":1701760331794,"prevProcessingTime":1701760337777,"error":{},"processingStartTime":1701760337092,"timespans":{"extractor":685}},"dataset":"d1"})
     */
  }

  private def validateBatchFailedEvents(batchFailedEvents: List[String]): Unit = {
    batchFailedEvents.size should be(1)
    //TODO: Add assertions for all 1 events
    /*
    (BatchFailedEvent,{"event":"{\"invalid_json\":\"{\\\"dataset\\\":\\\"d1\\\",\\\"event\\\":{\\\"id\\\":\\\"2\\\",\\\"vehicleCode\\\":\\\"HYUN-CRE-D6\\\",\\\"date\\\":\\\"2023-03-01\\\",\\\"dealer\\\":{\\\"dealerCode\\\":\\\"KUNUnited\\\",\\\"locationId\\\":\\\"KUN1\\\",\\\"email\\\":\\\"dealer1@gmail.com\\\",\\\"phone\\\":\\\"9849012345\\\"},\\\"metrics\\\":{\\\"bookingsTaken\\\":50,\\\"deliveriesPromised\\\":20,\\\"deliveriesDone\\\":19}}\"}","obsrv_meta":{"flags":{"extractor":"failed"},"syncts":1701758716432,"prevProcessingTime":1701758721945,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"extractor"},"error_code":"ERR_EXT_1018","error_msg":"Invalid JSON event, error while deserializing the event"},"processingStartTime":1701758721739,"timespans":{"extractor":206}},"invalid_json":"{\"dataset\":\"d1\",\"event\":{\"id\":\"2\",\"vehicleCode\":\"HYUN-CRE-D6\",\"date\":\"2023-03-01\",\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}}"})
     */
  }

  private def validateInvalidEvents(invalidEvents: List[String]): Unit = {
    invalidEvents.size should be(2)
    //TODO: Add assertions for all 2 events
    /*
    (FailedEvent,{"event":"{\"event\":{\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"6\",\"randomKey\":\"eRJcFJvUoQnlC9ZNa2b2NT84aAv4Trr9m6GFwxaL6Qn1srmWBl7ldsKnBvs6ah2l0KN6M3Vp4eoGLBiIMYsi3gHWklc8sbt6\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d1\"}","obsrv_meta":{"flags":{"extractor":"failed"},"syncts":1701758716560,"prevProcessingTime":1701758722479,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"extractor"},"error_code":"ERR_EXT_1003","error_msg":"Event size has exceeded max configured size of 1048576"},"processingStartTime":1701758721888,"timespans":{"extractor":591}},"dataset":"d1"})
    (FailedEvent,{"event":"{\"event\":{\"dealer\":{\"dealerCode\":\"KUNUnited\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"5\",\"randomKey\":\"eRJcFJvUoQnlC9ZNa2b2NT84aAv4Trr9m6GFwxaL6Qn1srmWBl7ldsKnBvs6ah2l0KN6M3Vp4eoGLBiIMYsi3gHWklc8sbt6\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d1\"}","obsrv_meta":{"flags":{"extractor":"failed"},"syncts":1701758716590,"prevProcessingTime":1701758722521,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"extractor"},"error_code":"ERR_EXT_1003","error_msg":"Event size has exceeded max configured size of 1048576"},"processingStartTime":1701758721888,"timespans":{"extractor":633}},"dataset":"d1"})
     */
  }

  private def validateSystemEvents(systemEvents: List[String]): Unit = {
    systemEvents.size should be(6)

    systemEvents.foreach(se => {
      val event = JSONUtil.deserialize[SystemEvent](se)
      if(event.ctx.dataset.getOrElse("ALL").equals("ALL"))
        event.ctx.dataset_type should be(None)
      else
        event.ctx.dataset_type.getOrElse("dataset") should be("dataset")
    })

    //TODO: Add assertions for all 6 events
    /*
    (SysEvent,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"ExtractorJob","type":"flink","pid":"extractor"},"dataset":"ALL"},"data":{"error":{"pdata_id":"extractor","pdata_status":"failed","error_type":"InvalidJsonData","error_code":"ERR_EXT_1018","error_message":"Invalid JSON event, error while deserializing the event","error_level":"critical"}},"ets":1701760337333})
    (SysEvent,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"ExtractorJob","type":"flink","pid":"extractor"},"dataset":"d1", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"dedup","pdata_status":"skipped","error_type":"DedupFailed","error_code":"ERR_DEDUP_1007","error_message":"No dedup key found or missing data","error_level":"warn"}},"ets":1701760337474})
    (SysEvent,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"ExtractorJob","type":"flink","pid":"extractor"},"dataset":"d1", "dataset_type": "dataset"},"data":{"pipeline_stats":{"extractor_events":1,"extractor_status":"success"}},"ets":1701760337655})
    (SysEvent,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"ExtractorJob","type":"flink","pid":"extractor"},"dataset":"d1", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"extractor","pdata_status":"failed","error_type":"EventSizeExceeded","error_code":"ERR_EXT_1003","error_message":"Event size has exceeded max configured size of 1048576","error_level":"critical"}},"ets":1701760337724})
    (SysEvent,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"ExtractorJob","type":"flink","pid":"extractor"},"dataset":"d1", "dataset_type": "dataset"},"data":{"error":{"pdata_id":"extractor","pdata_status":"failed","error_type":"EventSizeExceeded","error_code":"ERR_EXT_1003","error_message":"Event size has exceeded max configured size of 1048576","error_level":"critical"}},"ets":1701760337754})
    (SysEvent,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"ExtractorJob","type":"flink","pid":"extractor"},"dataset":"d1", "dataset_type": "dataset"},"data":{"pipeline_stats":{"extractor_events":1,"extractor_status":"success"}},"ets":1701760337754})
     */
  }

  private def validateMetrics(mutableMetricsMap: mutable.Map[String, Long]): Unit = {

    mutableMetricsMap(s"${pConfig.jobName}.ALL.${pConfig.eventFailedMetricsCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.totalEventCount}") should be(5)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.eventFailedMetricsCount}") should be(2)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.skippedExtractionCount}") should be(1)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.successEventCount}") should be(2)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.successExtractionCount}") should be(3)
  }

}