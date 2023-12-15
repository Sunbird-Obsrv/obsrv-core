package org.sunbird.obsrv.connector

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.sunbird.obsrv.BaseMetricsReporter
import org.sunbird.obsrv.connector.task.{KafkaConnectorConfig, KafkaConnectorStreamTask}
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil, PostgresConnect}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class KafkaConnectorStreamTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val pConfig = new KafkaConnectorConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(pConfig)
  val customKafkaConsumerProperties: Map[String, String] = Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-event-schema-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )
  implicit val deserializer: StringDeserializer = new StringDeserializer()
  private val VALID_JSON_EVENT = """{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}"""
  private val VALID_JSON_EVENT_ARRAY = """[{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}]"""
  private val INVALID_JSON_EVENT = """{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}"""

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
    EmbeddedKafka.publishStringMessageToKafka("d1-topic", VALID_JSON_EVENT)
    EmbeddedKafka.publishStringMessageToKafka("d2-topic", VALID_JSON_EVENT_ARRAY)
    EmbeddedKafka.publishStringMessageToKafka("d3-topic", INVALID_JSON_EVENT)

    flinkCluster.before()
  }

  private def prepareTestData(): Unit = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    postgresConnect.execute("insert into datasets(id, type, data_schema, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date, tags) values ('d3', 'dataset', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'System', 'System', now(), now(), ARRAY['Tag1','Tag2']);")
    postgresConnect.execute("insert into dataset_source_config values('sc1', 'd1', 'kafka', '{\"kafkaBrokers\":\"localhost:9093\",\"topic\":\"d1-topic\"}', 'Live', null, 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_source_config values('sc2', 'd1', 'rdbms', '{\"type\":\"postgres\",\"tableName\":\"test-table\"}', 'Live', null, 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_source_config values('sc3', 'd2', 'kafka', '{\"kafkaBrokers\":\"localhost:9093\",\"topic\":\"d2-topic\"}', 'Live', null, 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_source_config values('sc4', 'd3', 'kafka', '{\"kafkaBrokers\":\"localhost:9093\",\"topic\":\"d3-topic\"}', 'Live', null, 'System', 'System', now(), now());")
    postgresConnect.closeConnection()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      "d1-topic", "d2-topic", "d3-topic", pConfig.kafkaSystemTopic, "ingest"
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "KafkaConnectorStreamTestSpec" should "validate the kafka connector job" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(pConfig)
    val task = new KafkaConnectorStreamTask(pConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(pConfig.jobName)
    }

    val ingestEvents = EmbeddedKafka.consumeNumberMessagesFrom[String]("ingest", 3, timeout = 30.seconds)
    validateIngestEvents(ingestEvents)

    pConfig.inputTopic() should be ("")
    pConfig.inputConsumer() should be ("")
    pConfig.successTag().getId should be ("dummy-events")
    pConfig.failedEventsOutputTag().getId should be ("failed-events")
  }

  private def validateIngestEvents(ingestEvents: List[String]): Unit = {
    ingestEvents.size should be(3)
    ingestEvents.foreach{event: String => {
      if(event.contains(""""dataset":"d1"""")) {
        JSONUtil.getJsonType(event) should be ("OBJECT")
        val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](event)
        eventMap.get("dataset").get.asInstanceOf[String] should be ("d1")
        eventMap.get("syncts").isDefined should be (true)
        eventMap.contains("event") should be (true)
      } else if(event.contains(""""dataset":"d2"""")) {
        JSONUtil.getJsonType(event) should be("OBJECT")
        val eventMap = JSONUtil.deserialize[Map[String, AnyRef]](event)
        eventMap.get("dataset").get.asInstanceOf[String] should be("d2")
        eventMap.get("syncts").isDefined should be(true)
        eventMap.contains("events") should be(true)
        JSONUtil.getJsonType(JSONUtil.serialize(eventMap.get("events"))) should be("ARRAY")
      } else {
        JSONUtil.getJsonType(event) should be ("NOT_A_JSON")
        event.contains(""""event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealerCode":"KUNUnited","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}""") should be(true)
      }
    }}

  }

}