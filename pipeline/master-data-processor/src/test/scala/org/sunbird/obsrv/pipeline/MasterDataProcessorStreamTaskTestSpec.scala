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
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, PostgresConnect}
import org.sunbird.obsrv.fixture.EventFixture
import org.sunbird.obsrv.pipeline.task.{MasterDataProcessorConfig, MasterDataProcessorStreamTask}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class MasterDataProcessorStreamTaskTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val masterDataConfig = new MasterDataProcessorConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(masterDataConfig)
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
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.VALID_BATCH_EVENT_D3_INSERT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.VALID_BATCH_EVENT_D3_INSERT_2)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.VALID_BATCH_EVENT_D4)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.VALID_BATCH_EVENT_D3_UPDATE)
    flinkCluster.before()
  }

  private def insertTestData(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("insert into datasets(id, type, extraction_config, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date) values ('d3', 'master-dataset', '{\"is_batch_event\": true, \"extraction_key\": \"events\"}', '{\"topic\":\"d3-events\"}', '{\"data_key\":\"code\",\"timestamp_key\":\"date\",\"entry_topic\":\"masterdata.ingest\",\"redis_db_host\":\"localhost\",\"redis_db_port\":6340,\"redis_db\":3}', 'ACTIVE', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into datasets(id, type, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date) values ('d4', 'master-dataset', '{\"topic\":\"d4-events\"}', '{\"data_key\":\"code\",\"timestamp_key\":\"date\",\"entry_topic\":\"masterdata-ingest\",\"redis_db_host\":\"localhost\",\"redis_db_port\":6340,\"redis_db\":4}', 'ACTIVE', 'System', 'System', now(), now());")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      config.getString("kafka.stats.topic"), config.getString("kafka.output.transform.topic"), config.getString("kafka.output.duplicate.topic"),
      config.getString("kafka.output.unique.topic"), config.getString("kafka.output.invalid.topic"), config.getString("kafka.output.batch.failed.topic"), config.getString("kafka.output.failed.topic"),
      config.getString("kafka.output.extractor.duplicate.topic"), config.getString("kafka.output.raw.topic"), config.getString("kafka.input.topic")
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "MasterDataProcessorStreamTaskTestSpec" should "validate the entire master data pipeline" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(masterDataConfig)
    val task = new MasterDataProcessorStreamTask(config, masterDataConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(masterDataConfig.jobName)
    }

    val input = EmbeddedKafka.consumeNumberMessagesFrom[String](config.getString("kafka.stats.topic"), 4, timeout = 30.seconds)
    input.size should be (4)

    val mutableMetricsMap = mutable.Map[String, Long]();
    BaseMetricsReporter.gaugeMetrics.toMap.mapValues(f => f.getValue()).map(f => mutableMetricsMap.put(f._1, f._2))

    masterDataConfig.successTag().getId should be ("processing_stats")

    mutableMetricsMap(s"${masterDataConfig.jobName}.d3.${masterDataConfig.totalEventCount}") should be (3)
    mutableMetricsMap(s"${masterDataConfig.jobName}.d3.${masterDataConfig.successEventCount}") should be (3)
    mutableMetricsMap(s"${masterDataConfig.jobName}.d3.${masterDataConfig.successInsertCount}") should be (2)
    mutableMetricsMap(s"${masterDataConfig.jobName}.d3.${masterDataConfig.successUpdateCount}") should be (1)

    mutableMetricsMap(s"${masterDataConfig.jobName}.d4.${masterDataConfig.totalEventCount}") should be(1)
    mutableMetricsMap(s"${masterDataConfig.jobName}.d4.${masterDataConfig.successEventCount}") should be(1)
    mutableMetricsMap(s"${masterDataConfig.jobName}.d4.${masterDataConfig.successInsertCount}") should be(1)
    mutableMetricsMap(s"${masterDataConfig.jobName}.d4.${masterDataConfig.successUpdateCount}") should be(0)

    val redisConnection = new RedisConnect(masterDataConfig.redisHost, masterDataConfig.redisPort, masterDataConfig.redisConnectionTimeout)
    val jedis1 = redisConnection.getConnection(3)
    val event1 = jedis1.get("HYUN-CRE-D6")
    event1 should be ("""{"model":"Creta","price":"2200000","variant":"SX(O)","fuel":"Diesel","code":"HYUN-CRE-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic","seatingCapacity":5,"safety":"3 Star (Global NCAP)"}""")
    val event3 = jedis1.get("HYUN-TUC-D6")
    event3 should be ("""{"model":"Tucson","price":"4000000","variant":"Signature","fuel":"Diesel","code":"HYUN-TUC-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic"}""")
    jedis1.close()

    val jedis2 = redisConnection.getConnection(4)
    val event2 = jedis2.get("JEEP-CP-D3")
    event2 should be ("""{"model":"Compass","price":"3800000","variant":"Model S (O) Diesel 4x4 AT","fuel":"Diesel","seatingCapacity":5,"code":"JEEP-CP-D3","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Jeep","safety":"5 Star (Euro NCAP)","modelYear":"2023","transmission":"automatic"}""")
    jedis2.close()
    
  }


}
