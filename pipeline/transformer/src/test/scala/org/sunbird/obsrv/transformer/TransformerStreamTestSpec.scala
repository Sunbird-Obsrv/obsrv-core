package org.sunbird.obsrv.transformer

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.sunbird.obsrv.BaseMetricsReporter
import org.sunbird.obsrv.core.model.Models.SystemEvent
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil, PostgresConnect}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry
import org.sunbird.obsrv.transformer.task.{TransformerConfig, TransformerStreamTask}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class TransformerStreamTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val transformerConfig = new TransformerConfig(config)
  val redisPort: Int = transformerConfig.redisPort
  val kafkaConnector = new FlinkKafkaConnector(transformerConfig)
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
    insertTestData()
    createTestTopics()
    publishMessagesToKafka()
    flinkCluster.before()
  }

  private def publishMessagesToKafka(): Unit = {
    EmbeddedKafka.publishStringMessageToKafka(transformerConfig.inputTopic(), EventFixture.SUCCESS_TRANSFORM)
    EmbeddedKafka.publishStringMessageToKafka(transformerConfig.inputTopic(), EventFixture.FAILED_TRANSFORM)
    EmbeddedKafka.publishStringMessageToKafka(transformerConfig.inputTopic(), EventFixture.SKIPPED_TRANSFORM)
    EmbeddedKafka.publishStringMessageToKafka(transformerConfig.inputTopic(), EventFixture.PARTIAL_TRANSFORM)
    EmbeddedKafka.publishStringMessageToKafka(transformerConfig.inputTopic(), EventFixture.FAILED_TRANSFORM_2)
  }

  private def insertTestData(): Unit = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    postgresConnect.execute("insert into datasets(id, type, data_schema, router_config, dataset_config, status, api_version, entry_topic, created_by, updated_by, created_date, updated_date, tags) values ('d3', 'dataset', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'v1', 'ingest', 'System', 'System', now(), now(), ARRAY['Tag1','Tag2']);")
    postgresConnect.execute("insert into datasets(id, type, data_schema, router_config, dataset_config, status, api_version, entry_topic, created_by, updated_by, created_date, updated_date, tags) values ('d4', 'dataset', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'v1', 'ingest', 'System', 'System', now(), now(), ARRAY['Tag1','Tag2']);")
    postgresConnect.execute("insert into dataset_transformations values('tf3', 'd2', 'tfdata.valueAsInt', '{\"type\":\"jsonata\",\"expr\":\"$number(id)\"}', null, 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_transformations values('tf4', 'd2', 'tfdata.encryptEmail', '{\"type\":\"encrypt\",\"expr\": \"dealer.email\"}', 'Lenient', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_transformations values('tf5', 'd4', 'tfdata.expr1', '{\"type\":\"jsonata\",\"expr\":null}', null, 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_transformations values('tf6', 'd4', 'tfdata.expr2', '{\"type\":\"jsonata\",\"expr\":\"$keys(dealer).length\"}', null, 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_transformations values('tf7', 'd4', 'tfdata.expr3', '{\"type\":\"jsonata\",\"expr\":\"number(id)\"}', null, 'System', 'System', now(), now());")
    postgresConnect.closeConnection()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      transformerConfig.inputTopic(), transformerConfig.kafkaFailedTopic, transformerConfig.kafkaSystemTopic, transformerConfig.kafkaTransformTopic, transformerConfig.kafkaTransformFailedTopic
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "TransformerStreamTestSpec" should "validate the transform stream task" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(transformerConfig)
    val task = new TransformerStreamTask(transformerConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(transformerConfig.jobName)
    }

    val outputs = EmbeddedKafka.consumeNumberMessagesFrom[String](transformerConfig.kafkaTransformTopic, 3, timeout = 30.seconds)
    validateOutputs(outputs)

    val failedEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](transformerConfig.kafkaTransformFailedTopic, 2, timeout = 30.seconds)
    validateFailedEvents(failedEvents)

    val systemEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](transformerConfig.kafkaSystemTopic, 5, timeout = 30.seconds)
    validateSystemEvents(systemEvents)

    val mutableMetricsMap = mutable.Map[String, Long]()
    BaseMetricsReporter.gaugeMetrics.toMap.mapValues(f => f.getValue()).map(f => mutableMetricsMap.put(f._1, f._2))
    Console.println("### DenormalizerStreamTaskTestSpec:metrics ###", JSONUtil.serialize(getPrintableMetrics(mutableMetricsMap)))
    validateMetrics(mutableMetricsMap)

    transformerConfig.successTag().getId should be("transformed-events")
  }

  private def validateOutputs(outputs: List[String]): Unit = {
    outputs.size should be(3)
    outputs.zipWithIndex.foreach {
      case (elem, idx) =>
        val msg = JSONUtil.deserialize[Map[String, AnyRef]](elem)
        val event = JSONUtil.serialize(msg(Constants.EVENT))
        val obsrvMeta = msg(Constants.OBSRV_META).asInstanceOf[Map[String, AnyRef]]
        obsrvMeta("timespans").asInstanceOf[Map[String, AnyRef]]("transformer").asInstanceOf[Int] should be > 0
        idx match {
          case 0 =>
            event should be("""{"dealer":{"email":"de****1@gmail.com","maskedPhone":"98******45","locationId":"KUN1","dealerCode":"D123","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
            obsrvMeta("flags").asInstanceOf[Map[String, AnyRef]]("transformer").asInstanceOf[String] should be("success")
          case 1 =>
            event should be("""{"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
            obsrvMeta("flags").asInstanceOf[Map[String, AnyRef]]("transformer").asInstanceOf[String] should be("skipped")
          case 2 =>
            event should be("""{"tfdata":{"valueAsInt":1235},"dealer":{"dealerCode":"D123","locationId":"KUN1","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1235","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
            obsrvMeta("flags").asInstanceOf[Map[String, AnyRef]]("transformer").asInstanceOf[String] should be("partial")
        }
    }
    /*
    (Output Event,{"obsrv_meta":{"flags":{"transformer":"success"},"syncts":1701863209956,"prevProcessingTime":1701863215734,"error":{},"processingStartTime":1701863215322,"timespans":{"transformer":412}},"event":{"dealer":{"email":"de****1@gmail.com","maskedPhone":"98******45","locationId":"KUN1","dealerCode":"D123","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"dataset":"d1"},0)
    (Output Event,{"obsrv_meta":{"flags":{"transformer":"skipped"},"syncts":1701863210084,"prevProcessingTime":1701863216141,"error":{},"processingStartTime":1701863215476,"timespans":{"transformer":665}},"event":{"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"dataset":"d3"},1)
    (Output Event,{"obsrv_meta":{"flags":{"transformer":"partial"},"syncts":1701863210111,"prevProcessingTime":1701863216378,"error":{},"processingStartTime":1701863215477,"timespans":{"transformer":901}},"event":{"tfdata":{"valueAsInt":1235},"dealer":{"dealerCode":"D123","locationId":"KUN1","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1235","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}},"dataset":"d2"},2)
     */
  }

  private def validateFailedEvents(failedEvents: List[String]): Unit = {
    failedEvents.size should be(2)
    failedEvents.zipWithIndex.foreach {
      case (elem, idx) =>
        val msg = JSONUtil.deserialize[Map[String, AnyRef]](elem)
        val event = msg(Constants.EVENT).asInstanceOf[String]
        val obsrvMeta = msg(Constants.OBSRV_META).asInstanceOf[Map[String, AnyRef]]
        obsrvMeta("timespans").asInstanceOf[Map[String, AnyRef]]("transformer").asInstanceOf[Int] should be > 0
        obsrvMeta("flags").asInstanceOf[Map[String, AnyRef]]("transformer").asInstanceOf[String] should be (StatusCode.failed.toString)
        obsrvMeta("error").asInstanceOf[Map[String, AnyRef]]("src").asInstanceOf[String] should be (Producer.transformer.toString)
        obsrvMeta("error").asInstanceOf[Map[String, AnyRef]]("error_code").asInstanceOf[String] should be (ErrorConstants.ERR_TRANSFORMATION_FAILED.errorCode)
        idx match {
          case 0 =>
            event should be("{\"event\":{\"dealer\":{\"maskedPhone\":\"98******45\",\"locationId\":\"KUN1\",\"dealerCode\":\"D123\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1235\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d1\"}")
          case 1 =>
            event should be("{\"event\":{\"tfdata\":{},\"dealer\":{\"dealerCode\":\"D123\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1234\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d4\"}")
        }
    }
    /*
    (Failed Event,{"event":"{\"event\":{\"dealer\":{\"maskedPhone\":\"98******45\",\"locationId\":\"KUN1\",\"dealerCode\":\"D123\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1235\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d1\"}","obsrv_meta":{"flags":{"transformer":"failed"},"syncts":1701863210058,"prevProcessingTime":1701863215948,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"transformer"},"error_code":"ERR_TRANSFORM_1023","error_msg":"Atleast one mandatory transformation has failed"},"processingStartTime":1701863215475,"timespans":{"transformer":473}},"dataset":"d1"},0)
    (Failed Event,{"event":"{\"event\":{\"tfdata\":{},\"dealer\":{\"dealerCode\":\"D123\",\"locationId\":\"KUN1\",\"email\":\"dealer1@gmail.com\",\"phone\":\"9849012345\"},\"vehicleCode\":\"HYUN-CRE-D6\",\"id\":\"1234\",\"date\":\"2023-03-01\",\"metrics\":{\"bookingsTaken\":50,\"deliveriesPromised\":20,\"deliveriesDone\":19}},\"dataset\":\"d4\"}","obsrv_meta":{"flags":{"transformer":"failed"},"syncts":1701863210150,"prevProcessingTime":1701863216421,"error":{"src":{"enumClass":"org.sunbird.obsrv.core.model.Producer","value":"transformer"},"error_code":"ERR_TRANSFORM_1023","error_msg":"Atleast one mandatory transformation has failed"},"processingStartTime":1701863215477,"timespans":{"transformer":944}},"dataset":"d4"},1)
     */
  }

  private def validateSystemEvents(systemEvents: List[String]): Unit = {
    systemEvents.size should be(5)
    systemEvents.count(f => {
      val event = JSONUtil.deserialize[SystemEvent](f)
      FunctionalError.TransformFieldMissing.equals(event.data.error.get.error_type)
    }) should be(2)
    systemEvents.count(f => {
      val event = JSONUtil.deserialize[SystemEvent](f)
      FunctionalError.TransformFailedError.equals(event.data.error.get.error_type)
    }) should be(1)
    systemEvents.count(f => {
      val event = JSONUtil.deserialize[SystemEvent](f)
      FunctionalError.TransformEvalError.equals(event.data.error.get.error_type)
    }) should be(1)
    systemEvents.count(f => {
      val event = JSONUtil.deserialize[SystemEvent](f)
      FunctionalError.TransformParseError.equals(event.data.error.get.error_type)
    }) should be(1)

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
    // TODO: Add more assertions
    /*
    (Sys Event,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"TransformerJob","type":"flink","pid":"denorm"},"dataset":"d1"},"data":{"error":{"pdata_id":"denorm","pdata_status":"failed","error_type":"TransformFieldMissing","error_code":"ERR_TRANSFORM_1023","error_message":"Transformation field is either missing or blank","error_level":"critical","error_count":1}},"ets":1701863215985},0)
    (Sys Event,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"TransformerJob","type":"flink","pid":"denorm"},"dataset":"d2"},"data":{"error":{"pdata_id":"denorm","pdata_status":"failed","error_type":"TransformFieldMissing","error_code":"ERR_TRANSFORM_1023","error_message":"Transformation field is either missing or blank","error_level":"critical","error_count":1}},"ets":1701863216391},1)
    (Sys Event,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"TransformerJob","type":"flink","pid":"denorm"},"dataset":"d4"},"data":{"error":{"pdata_id":"denorm","pdata_status":"failed","error_type":"TransformFailedError","error_code":"ERR_TRANSFORM_1022","error_message":"Unable to evaluate the transformation expression function","error_level":"critical","error_count":1}},"ets":1701863216431},2)
    (Sys Event,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"TransformerJob","type":"flink","pid":"denorm"},"dataset":"d4"},"data":{"error":{"pdata_id":"denorm","pdata_status":"failed","error_type":"TransformEvalError","error_code":"ERR_TRANSFORM_1021","error_message":"Unable to evaluate the transformation expression function","error_level":"critical","error_count":1}},"ets":1701863216433},3)
    (Sys Event,{"etype":"METRIC","ctx":{"module":"processing","pdata":{"id":"TransformerJob","type":"flink","pid":"denorm"},"dataset":"d4"},"data":{"error":{"pdata_id":"denorm","pdata_status":"failed","error_type":"TransformParseError","error_code":"ERR_TRANSFORM_1020","error_message":"Transformation expression function is not valid","error_level":"critical","error_count":1}},"ets":1701863216433},4)
     */
  }

  private def validateMetrics(mutableMetricsMap: mutable.Map[String, Long]): Unit = {
    mutableMetricsMap(s"${transformerConfig.jobName}.d1.${transformerConfig.totalEventCount}") should be(2)
    mutableMetricsMap(s"${transformerConfig.jobName}.d1.${transformerConfig.transformSuccessCount}") should be(1)
    mutableMetricsMap(s"${transformerConfig.jobName}.d1.${transformerConfig.transformFailedCount}") should be(1)

    mutableMetricsMap(s"${transformerConfig.jobName}.d2.${transformerConfig.totalEventCount}") should be(1)
    mutableMetricsMap(s"${transformerConfig.jobName}.d2.${transformerConfig.transformPartialCount}") should be(1)

    mutableMetricsMap(s"${transformerConfig.jobName}.d3.${transformerConfig.totalEventCount}") should be(1)
    mutableMetricsMap(s"${transformerConfig.jobName}.d3.${transformerConfig.transformSkippedCount}") should be(1)

    mutableMetricsMap(s"${transformerConfig.jobName}.d4.${transformerConfig.totalEventCount}") should be(1)
    mutableMetricsMap(s"${transformerConfig.jobName}.d4.${transformerConfig.transformFailedCount}") should be(1)
  }

}