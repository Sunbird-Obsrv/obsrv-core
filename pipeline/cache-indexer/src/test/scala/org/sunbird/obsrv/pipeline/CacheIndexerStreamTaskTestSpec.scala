package org.sunbird.obsrv.pipeline

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
import org.sunbird.obsrv.fixture.EventFixture
import org.sunbird.obsrv.pipeline.task.CacheIndexerConfig
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry
import org.sunbird.obsrv.streaming.CacheIndexerStreamTask

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class CacheIndexerStreamTaskTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val cacheIndexerConfig = new CacheIndexerConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(cacheIndexerConfig)
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
    val postgresConnect = new PostgresConnect(postgresConfig)
    insertTestData(postgresConnect)
    createTestTopics()
    EmbeddedKafka.publishStringMessageToKafka("dataset3", EventFixture.VALID_BATCH_EVENT_D3_INSERT)
    EmbeddedKafka.publishStringMessageToKafka("dataset3", EventFixture.VALID_BATCH_EVENT_D3_INSERT_2)
    EmbeddedKafka.publishStringMessageToKafka("dataset4", EventFixture.VALID_BATCH_EVENT_D4)
    EmbeddedKafka.publishStringMessageToKafka("dataset3", EventFixture.VALID_BATCH_EVENT_D3_UPDATE)
    EmbeddedKafka.publishStringMessageToKafka("dataset4", EventFixture.INVALID_BATCH_EVENT_D4)
    flinkCluster.before()
  }

  private def insertTestData(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("INSERT INTO datasets (id, type, validation_config, extraction_config, data_schema, router_config, dataset_config, status, api_version, entry_topic, created_by, updated_by, created_date, updated_date) VALUES ('dataset3', 'master', '{\"validate\":true,\"mode\":\"Strict\"}', '{\"is_batch_event\":true,\"extraction_key\":\"events\"}', '{\"type\":\"object\",\"$schema\":\"http://json-schema.org/draft-04/schema#\",\"properties\":{\"code\":{\"type\":\"string\"},\"manufacturer\":{\"type\":\"string\"}, \"model\":{\"type\":\"string\"},\"variant\":{\"type\":\"string\"},\"modelYear\":{\"type\":\"string\"},\"price\":{\"type\":\"string\"},\"currencyCode\":{\"type\":\"string\"},\"currency\":{\"type\":\"string\"},\"transmission\":{\"type\":\"string\"},\"fuel\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"}}}}}', '{\"topic\":\"d3-events\"}', '{\"data_key\":\"code\",\"timestamp_key\":\"date\",\"entry_topic\":\"local.masterdata.ingest\",\"redis_db\":3,\"redis_db_host\":\"localhost\",\"redis_db_port\":" + cacheIndexerConfig.redisPort + "}', 'Live', 'v1', 'local.masterdata.ingest', 'System', 'System',  now(), now());")
    postgresConnect.execute("INSERT INTO datasets (id, type, validation_config, data_schema, router_config, dataset_config, status, api_version, entry_topic, created_by, updated_by, created_date, updated_date) VALUES ('dataset4', 'master', '{\"validate\":true,\"mode\":\"Strict\"}', '{\"type\":\"object\",\"$schema\":\"http://json-schema.org/draft-04/schema#\",\"properties\":{\"code\":{\"type\":\"string\"},\"manufacturer\":{\"type\":\"string\"},\"model\":{\"type\":\"string\"},\"variant\":{\"type\":\"string\"},\"modelYear\":{\"type\":\"string\"},\"price\":{\"type\":\"string\"},\"currencyCode\":{\"type\":\"string\"},\"currency\":{\"type\":\"string\"},\"seatingCapacity\": {\"type\": \"integer\"}, \"safety\": {\"type\": \"string\"},\"transmission\":{\"type\":\"string\"},\"fuel\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"email\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"}}}}}', '{\"topic\":\"d34-events\"}', '{\"indexing_config\":{\"olap_store_enabled\":false,\"lakehouse_enabled\":false,\"cache_enabled\":true},\"keys_config\":{\"data_key\":\"code\",\"timestamp_key\":\"date\"},\"cache_config\":{\"redis_db\":4,\"redis_db_host\":\"localhost\",\"redis_db_port\":" + cacheIndexerConfig.redisPort + "}}', 'Live', 'v2', 'local.masterdata.ingest', 'System', 'System',  now(), now());")
  }

  override def afterAll(): Unit = {

    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(config.getString("kafka.output.system.event.topic"), "dataset3", "dataset4").foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "CacheIndexerStreamTaskTestSpec" should "validate the cache indexer job for master datasets" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(cacheIndexerConfig)
    val task = new CacheIndexerStreamTask(cacheIndexerConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(cacheIndexerConfig.jobName)
    }

    val input = EmbeddedKafka.consumeNumberMessagesFrom[String](config.getString("kafka.output.system.event.topic"), 1, timeout = 30.seconds)
    input.size should be(1)

    input.foreach(se => {
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
        event.ctx.dataset_type should be(Some("master"))
    })

    val mutableMetricsMap = mutable.Map[String, Long]();
    BaseMetricsReporter.gaugeMetrics.toMap.mapValues(f => f.getValue()).map(f => mutableMetricsMap.put(f._1, f._2))

    cacheIndexerConfig.successTag().getId should be("processing_stats")

    mutableMetricsMap(s"${cacheIndexerConfig.jobName}.dataset3.${cacheIndexerConfig.totalEventCount}") should be(3)
    mutableMetricsMap(s"${cacheIndexerConfig.jobName}.dataset3.${cacheIndexerConfig.successEventCount}") should be(3)
    mutableMetricsMap(s"${cacheIndexerConfig.jobName}.dataset3.${cacheIndexerConfig.successInsertCount}") should be(2)
    mutableMetricsMap(s"${cacheIndexerConfig.jobName}.dataset3.${cacheIndexerConfig.successUpdateCount}") should be(1)

    mutableMetricsMap(s"${cacheIndexerConfig.jobName}.dataset4.${cacheIndexerConfig.totalEventCount}") should be(2)
    mutableMetricsMap(s"${cacheIndexerConfig.jobName}.dataset4.${cacheIndexerConfig.successEventCount}") should be(1)
    mutableMetricsMap(s"${cacheIndexerConfig.jobName}.dataset4.${cacheIndexerConfig.successInsertCount}") should be(1)
    mutableMetricsMap(s"${cacheIndexerConfig.jobName}.dataset4.${cacheIndexerConfig.eventFailedMetricsCount}") should be(1)

    val redisConnection = new RedisConnect(cacheIndexerConfig.redisHost, cacheIndexerConfig.redisPort, cacheIndexerConfig.redisConnectionTimeout)
    val jedis1 = redisConnection.getConnection(3)
    val event1 = jedis1.get("HYUN-CRE-D6")
    event1 should be("""{"dealer":{"email":"john.doe@example.com","locationId":"KUN12345"},"model":"Creta","price":"2200000","variant":"SX(O)","fuel":"Diesel","code":"HYUN-CRE-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic","safety":"3 Star (Global NCAP)","seatingCapacity":5}""")
    val event3 = jedis1.get("HYUN-TUC-D6")
    event3 should be("""{"dealer":{"email":"admin.hyun@gmail.com","locationId":"KUN134567"},"model":"Tucson","price":"4000000","variant":"Signature","fuel":"Diesel","code":"HYUN-TUC-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic"}""")
    jedis1.close()

    val jedis2 = redisConnection.getConnection(4)
    val event2 = jedis2.get("JEEP-CP-D3")
    event2 should be("""{"model":"Compass","price":"3800000","variant":"Model S (O) Diesel 4x4 AT","fuel":"Diesel","seatingCapacity":5,"code":"JEEP-CP-D3","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Jeep","safety":"5 Star (Euro NCAP)","modelYear":"2023","transmission":"automatic"}""")
    jedis2.close()
  }


}
