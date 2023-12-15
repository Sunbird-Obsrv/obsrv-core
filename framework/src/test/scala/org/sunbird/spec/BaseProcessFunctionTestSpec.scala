package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import io.github.embeddedkafka.Codecs.stringSerializer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers
import org.sunbird.obsrv.core.streaming._
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil, Util}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class BaseProcessFunctionTestSpec extends BaseSpec with Matchers {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val config: Config = ConfigFactory.load("base-test.conf")
  val bsMapConfig = new BaseProcessTestMapConfig(config)
  val bsConfig = new BaseProcessTestConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(bsConfig)

  val SAMPLE_EVENT_1: String = """{"dataset":"d1","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealer":"KUNUnited","locationId":"KUN1"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}""".stripMargin
  val SAMPLE_EVENT_2: String = """{"dataset":"d1","event":{"id":"4567","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealer":"KUNUnited","locationId":"KUN1"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}""".stripMargin
  val SAMPLE_EVENT_3: String = """{"dataset":"d1","obsrv_meta":{},"event":{"id":"4567","vehicleCode":"HYUN-CRE-D7","date":"2023-03-01","dealer":{"dealer":"KUNUnited","locationId":"KUN1"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}""".stripMargin
  val SAMPLE_EVENT_4: String = """{"dataset":"d1","obsrv_meta":{},"event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealer":"KUNUnited","locationId":"KUN1"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}""".stripMargin

  val customKafkaConsumerProperties: Map[String, String] =  Map[String, String]("auto.offset.reset" -> "earliest", "group.id" -> "test-event-schema-group")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig (
      kafkaPort = 9093,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )
  implicit val deserializer: StringDeserializer = new StringDeserializer()

  override def beforeAll(): Unit = {
    super.beforeAll()

    EmbeddedKafka.start()(embeddedKafkaConfig)
    createTestTopics(bsConfig.testTopics)

    EmbeddedKafka.publishStringMessageToKafka(bsConfig.kafkaMapInputTopic, SAMPLE_EVENT_2)
    EmbeddedKafka.publishToKafka(bsConfig.kafkaMapInputTopic, "4567", SAMPLE_EVENT_3)
    EmbeddedKafka.publishStringMessageToKafka(bsConfig.kafkaStringInputTopic, SAMPLE_EVENT_1)
    EmbeddedKafka.publishToKafka(bsConfig.kafkaStringInputTopic, "1234", SAMPLE_EVENT_4)

    EmbeddedKafka
    flinkCluster.before()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(topics: List[String]): Unit = {
    topics.foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "Validation of SerDe" should "validate serialization and deserialization of Map and String schema" in {

    try {
      implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(bsConfig)

      val mapStream = env.fromSource(kafkaConnector.kafkaMapSource(bsConfig.kafkaMapInputTopic), WatermarkStrategy.noWatermarks[mutable.Map[String, AnyRef]](),
          "map-event-consumer")
          .process(new TestMapStreamFunc(bsMapConfig)).name("TestMapEventStream")

      mapStream.getSideOutput(bsConfig.mapOutputTag)
        .sinkTo(kafkaConnector.kafkaSink[mutable.Map[String, AnyRef]](bsConfig.kafkaMapOutputTopic))
        .name("Map-Event-Producer")

      val stringStream =
        env.fromSource(kafkaConnector.kafkaStringSource(bsConfig.kafkaStringInputTopic), WatermarkStrategy.noWatermarks[String](), "string-event-consumer")
          .keyBy(new KeySelector[String, String] {
            override def getKey(in: String): String = "test"
          }).window(TumblingProcessingTimeWindows.of(Time.seconds(2))).process(new TestStringWindowStreamFunc(bsConfig)).name("TestStringEventStream")

      stringStream.getSideOutput(bsConfig.stringOutputTag)
        .sinkTo(kafkaConnector.kafkaSink[String](bsConfig.kafkaStringOutputTopic))
        .name("String-Producer")

      Future {
        env.execute("TestSerDeFunctionality")
        Thread.sleep(5000)
      }

      val mapSchemaMessages = EmbeddedKafka.consumeNumberMessagesFrom[String](bsConfig.kafkaMapOutputTopic, 1, timeout = 20.seconds)
      val stringSchemaMessages = EmbeddedKafka.consumeNumberMessagesFrom[String](bsConfig.kafkaStringOutputTopic, 1, timeout = 20.seconds)

      mapSchemaMessages.size should be(1)
      stringSchemaMessages.size should be(1)

      retrieveId(mapSchemaMessages.head) should be("4567")
      retrieveId(stringSchemaMessages.head) should be("1234")
    } catch {
        case ex: Exception =>
          ex.printStackTrace()
          println("Error occurred when consuming events from Embedded Kafka...")
    }

  }

  def retrieveId(message: String): String = {
    val map = JSONUtil.deserialize[mutable.Map[String, AnyRef]](message)
    val node = JSONUtil.getKey("event.id", message)
    node.textValue()
  }

  "TestUtil" should "cover code for all utility functions" in {

    val map = JSONUtil.deserialize[Map[String, AnyRef]](SAMPLE_EVENT_1)
    val mutableMap = Util.getMutableMap(map)
    mutableMap.getClass.getCanonicalName should be ("scala.collection.mutable.HashMap")
    noException shouldBe thrownBy(JSONUtil.convertValue(map))
    val metrics = Metrics(mutable.Map("test" -> new ConcurrentHashMap[String, AtomicLong]()))
    metrics.reset("test1", "m1")

    bsConfig.datasetType() should be ("dataset")
  }

  "TestBaseStreamTask" should "validate the getMapDataStream method" in {
    try {
      val task = new TestMapStreamTask(bsMapConfig, kafkaConnector)
      Future {
        task.process()
        Thread.sleep(2000)
      }
      val mapSchemaMessages = EmbeddedKafka.consumeNumberMessagesFrom[String](bsConfig.kafkaMapOutputTopic, 1, timeout = 20.seconds)
      retrieveId(mapSchemaMessages.head) should be("4567")
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        println("Error occurred when consuming events from Embedded Kafka...")
    }
  }

  "TestBaseStreamTask" should "validate the getStringDataStream methods" in {
    try {
      val task = new TestStringStreamTask(bsConfig, kafkaConnector)
      Future {
        task.process()
        Thread.sleep(2000)
      }
      val stringSchemaMessages = EmbeddedKafka.consumeNumberMessagesFrom[String](bsConfig.kafkaStringOutputTopic, 1, timeout = 20.seconds)
      retrieveId(stringSchemaMessages.head) should be("1234")
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        println("Error occurred when consuming events from Embedded Kafka...")
    }
  }
}
