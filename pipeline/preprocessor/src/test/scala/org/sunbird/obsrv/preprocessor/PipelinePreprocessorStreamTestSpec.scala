package org.sunbird.obsrv.preprocessor

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.sunbird.obsrv.BaseMetricsReporter
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.FlinkUtil
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
    createTestTopics()
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.VALID_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.INVALID_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.DUPLICATE_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.MISSING_DATASET_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.INVALID_DATASET_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.INVALID_EVENT_KEY)
    EmbeddedKafka.publishStringMessageToKafka(pConfig.kafkaInputTopic, EventFixtures.VALID_EVENT_DEDUP_CONFIG_NONE)

    flinkCluster.before()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      pConfig.kafkaInputTopic, pConfig.kafkaInvalidTopic, pConfig.kafkaSystemTopic,
      pConfig.kafkaDuplicateTopic, pConfig.kafkaUniqueTopic
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
    //val extractorFailed = EmbeddedKafka.consumeNumberMessagesFrom[String](config.getString("kafka.input.topic"), 2, timeout = 60.seconds)
    val uniqueEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](pConfig.kafkaUniqueTopic, 1, timeout = 20.seconds)
    uniqueEvents.foreach(Console.println("Event:", _))

    val mutableMetricsMap = mutable.Map[String, Long]();
    val metricsMap = BaseMetricsReporter.gaugeMetrics.toMap.mapValues(f => f.getValue()).map(f => mutableMetricsMap.put(f._1, f._2))

    mutableMetricsMap(s"${pConfig.jobName}.ALL.${pConfig.validationTotalMetricsCount}") should be (7)
    mutableMetricsMap(s"${pConfig.jobName}.ALL.${pConfig.eventFailedMetricsCount}") should be (2)
    mutableMetricsMap(s"${pConfig.jobName}.ALL.${pConfig.duplicationTotalMetricsCount}") should be (3)

    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.validationFailureMetricsCount}") should be (1)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.duplicationProcessedEventMetricsCount}") should be (1)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.duplicationEventMetricsCount}") should be (1)
    mutableMetricsMap(s"${pConfig.jobName}.d1.${pConfig.validationSuccessMetricsCount}") should be (2)

    mutableMetricsMap(s"${pConfig.jobName}.d2.${pConfig.duplicationSkippedEventMetricsCount}") should be (1)
    mutableMetricsMap(s"${pConfig.jobName}.d2.${pConfig.validationSkipMetricsCount}") should be (1)
    mutableMetricsMap(s"${pConfig.jobName}.d2.${pConfig.eventFailedMetricsCount}") should be (1)

  }


}
