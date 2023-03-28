package org.sunbird.obsrv.pipeline

import com.typesafe.config.{Config, ConfigFactory}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.kafka.common.serialization.StringDeserializer
import org.sunbird.obsrv.BaseMetricsReporter
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.fixture.EventFixture
import org.sunbird.obsrv.pipeline.task.{MergedPipelineConfig, MergedPipelineStreamTask}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class MergedPipelineStreamTaskTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val mergedPipelineConfig = new MergedPipelineConfig(config)
  //val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val kafkaConnector = new FlinkKafkaConnector(mergedPipelineConfig)
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
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.EVENT_1)
    flinkCluster.before()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      config.getString("kafka.stats.topic"), config.getString("kafka.output.transform.topic"), config.getString("kafka.output.denorm.failed.topic"),
      config.getString("kafka.output.denorm.topic"), config.getString("kafka.output.duplicate.topic"), config.getString("kafka.output.unique.topic"),
      config.getString("kafka.output.invalid.topic"), config.getString("kafka.output.batch.failed.topic"), config.getString("kafka.output.failed.topic"),
      config.getString("kafka.output.extractor.duplicate.topic"), config.getString("kafka.output.raw.topic"), config.getString("kafka.input.topic")
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "MergedPipelineStreamTaskTestSpec" should "validate the entire pipeline" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(mergedPipelineConfig)
    val task = new MergedPipelineStreamTask(config, mergedPipelineConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(mergedPipelineConfig.jobName)
      Thread.sleep(10000)
    }
    //val extractorFailed = EmbeddedKafka.consumeNumberMessagesFrom[String](config.getString("kafka.input.topic"), 2, timeout = 60.seconds)
    val stats = EmbeddedKafka.consumeNumberMessagesFrom[String](mergedPipelineConfig.kafkaStatsTopic, 1, timeout = 20.seconds)
    stats.foreach(Console.println("Stats:", _))

  }


}
