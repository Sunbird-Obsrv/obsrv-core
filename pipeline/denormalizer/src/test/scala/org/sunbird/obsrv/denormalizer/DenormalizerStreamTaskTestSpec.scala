package org.sunbird.obsrv.denormalizer

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
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil, PostgresConnect}
import org.sunbird.obsrv.denormalizer.task.{DenormalizerConfig, DenormalizerStreamTask}
import org.sunbird.obsrv.denormalizer.util.DenormCache
import org.sunbird.obsrv.model.DatasetModels._
import org.sunbird.obsrv.model.DatasetStatus
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class DenormalizerStreamTaskTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val denormConfig = new DenormalizerConfig(config)
  val redisPort: Int = denormConfig.redisPort
  val kafkaConnector = new FlinkKafkaConnector(denormConfig)
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
    postgresConnect.closeConnection()
    createTestTopics()
    publishMessagesToKafka()
    flinkCluster.before()
  }

  private def publishMessagesToKafka(): Unit = {
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.SUCCESS_DENORM)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.SKIP_DENORM)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.DENORM_MISSING_KEYS)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.DENORM_MISSING_DATA_AND_INVALIDKEY)
  }

  private def insertTestData(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("update datasets set denorm_config = '" + s"""{"redis_db_host":"localhost","redis_db_port":$redisPort,"denorm_fields":[{"denorm_key":"vehicleCode","redis_db":3,"denorm_out_field":"vehicle_data"},{"denorm_key":"dealer.dealerCode","redis_db":4,"denorm_out_field":"dealer_data"}]}""" + "' where id='d1';")
    val redisConnection = new RedisConnect(denormConfig.redisHost, denormConfig.redisPort, denormConfig.redisConnectionTimeout)
    redisConnection.getConnection(3).set("HYUN-CRE-D6", EventFixture.DENORM_DATA_1)
    redisConnection.getConnection(4).set("D123", EventFixture.DENORM_DATA_2)
  }

  override def afterAll(): Unit = {
    val redisConnection = new RedisConnect(denormConfig.redisHost, denormConfig.redisPort, denormConfig.redisConnectionTimeout)
    redisConnection.getConnection(3).flushAll()
    redisConnection.getConnection(4).flushAll()

    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      config.getString("kafka.output.system.event.topic"), config.getString("kafka.output.denorm.topic"), config.getString("kafka.input.topic")
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "DenormalizerStreamTaskTestSpec" should "validate the denorm stream task" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(denormConfig)
    val task = new DenormalizerStreamTask(denormConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(denormConfig.jobName)
    }

    val outputs = EmbeddedKafka.consumeNumberMessagesFrom[String](denormConfig.denormOutputTopic, 4, timeout = 30.seconds)
    validateOutputs(outputs)

    val systemEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](denormConfig.kafkaSystemTopic, 3, timeout = 30.seconds)
    validateSystemEvents(systemEvents)

    val mutableMetricsMap = mutable.Map[String, Long]()
    BaseMetricsReporter.gaugeMetrics.toMap.mapValues(f => f.getValue()).map(f => mutableMetricsMap.put(f._1, f._2))
    Console.println("### DenormalizerStreamTaskTestSpec:metrics ###", JSONUtil.serialize(getPrintableMetrics(mutableMetricsMap)))
    validateMetrics(mutableMetricsMap)
  }

  it should "validate dynamic cache creation within DenormCache" in {
    val denormCache = new DenormCache(denormConfig)
    noException should be thrownBy {
      denormCache.open(Dataset(id = "d123", datasetType = "dataset", extractionConfig = None, dedupConfig = None, validationConfig = None, jsonSchema = None,
        denormConfig = Some(DenormConfig(redisDBHost = "localhost", redisDBPort = redisPort, denormFields = List(DenormFieldConfig(denormKey = "vehicleCode", redisDB = 3, denormOutField = "vehicle_data")))), routerConfig = RouterConfig(""),
        datasetConfig = DatasetConfig(key = "id", tsKey = "date", entryTopic = "ingest"), status = DatasetStatus.Live))
    }
  }

  private def validateOutputs(outputs: List[String]): Unit = {
    outputs.size should be(4)
    outputs.zipWithIndex.foreach {
      case (elem, idx) =>
        val msg = JSONUtil.deserialize[Map[String, AnyRef]](elem)
        val event = JSONUtil.serialize(msg(Constants.EVENT))
        idx match {
          case 0 => event should be("""{"vehicle_data":{"model":"Creta","price":"2200000","variant":"SX(O)","fuel":"Diesel","code":"HYUN-CRE-D6","currencyCode":"INR","currency":"Indian Rupee","manufacturer":"Hyundai","modelYear":"2023","transmission":"automatic"},"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","dealer_data":{"code":"D123","name":"KUN United","licenseNumber":"1234124","authorized":"yes"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 1 => event should be("""{"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":"HYUN-CRE-D6","id":"1234","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 2 => event should be("""{"dealer":{"dealerCode":"D123","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"id":"2345","date":"2023-03-01","dealer_data":{"code":"D123","name":"KUN United","licenseNumber":"1234124","authorized":"yes"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
          case 3 => event should be("""{"dealer":{"dealerCode":"D124","locationId":"KUN1","email":"dealer1@gmail.com","phone":"9849012345"},"vehicleCode":["HYUN-CRE-D7"],"id":"4567","date":"2023-03-01","metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}""")
        }
    }
  }

  private def validateSystemEvents(systemEvents: List[String]): Unit = {
    systemEvents.size should be(3)
    systemEvents.foreach(f => {
      val event = JSONUtil.deserialize[SystemEvent](f)
      event.etype should be(EventID.METRIC)
      event.ctx.module should be(ModuleID.processing)
      event.ctx.pdata.id should be(denormConfig.jobName)
      event.ctx.pdata.`type` should be(PDataType.flink)
      event.ctx.pdata.pid.get should be(Producer.denorm)
      event.data.error.isDefined should be(true)
      val errorLog = event.data.error.get
      errorLog.error_level should be(ErrorLevel.critical)
      errorLog.pdata_id should be(Producer.denorm)
      errorLog.pdata_status should be(StatusCode.failed)
      errorLog.error_count.get should be(1)
      errorLog.error_code match {
        case ErrorConstants.DENORM_KEY_MISSING.errorCode =>
          errorLog.error_type should be(FunctionalError.DenormKeyMissing)
        case ErrorConstants.DENORM_KEY_NOT_A_STRING_OR_NUMBER.errorCode =>
          errorLog.error_type should be(FunctionalError.DenormKeyInvalid)
        case ErrorConstants.DENORM_DATA_NOT_FOUND.errorCode =>
          errorLog.error_type should be(FunctionalError.DenormDataNotFound)
      }
    })
  }

  private def validateMetrics(mutableMetricsMap: mutable.Map[String, Long]): Unit = {
    mutableMetricsMap(s"${denormConfig.jobName}.d1.${denormConfig.denormTotal}") should be(3)
    mutableMetricsMap(s"${denormConfig.jobName}.d1.${denormConfig.denormFailed}") should be(1)
    mutableMetricsMap(s"${denormConfig.jobName}.d1.${denormConfig.denormSuccess}") should be(1)
    mutableMetricsMap(s"${denormConfig.jobName}.d1.${denormConfig.denormPartialSuccess}") should be(1)
    mutableMetricsMap(s"${denormConfig.jobName}.d2.${denormConfig.denormTotal}") should be(1)
    mutableMetricsMap(s"${denormConfig.jobName}.d2.${denormConfig.eventsSkipped}") should be(1)
  }

}
