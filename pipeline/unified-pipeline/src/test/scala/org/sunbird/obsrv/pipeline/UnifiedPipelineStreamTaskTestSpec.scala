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
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil}
import org.sunbird.obsrv.pipeline.task.{UnifiedPipelineConfig, UnifiedPipelineStreamTask}
import org.sunbird.obsrv.spec.BaseSpecWithDatasetRegistry

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class UnifiedPipelineStreamTaskTestSpec extends BaseSpecWithDatasetRegistry {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  val unifiedPipelineConfig = new UnifiedPipelineConfig(config)
  val kafkaConnector = new FlinkKafkaConnector(unifiedPipelineConfig)
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
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.VALID_BATCH_EVENT_D1)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.MISSING_DATASET_BATCH_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.UNREGISTERED_DATASET_BATCH_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.DUPLICATE_BATCH_EVENT)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.INVALID_BATCH_EVENT_INCORRECT_EXTRACTION_KEY)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.INVALID_BATCH_EVENT_EXTRACTION_KEY_NOT_ARRAY)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.VALID_BATCH_EVENT_D2)
    EmbeddedKafka.publishStringMessageToKafka(config.getString("kafka.input.topic"), EventFixture.INVALID_BATCH_EVENT_D2)
    flinkCluster.before()
  }

  override def afterAll(): Unit = {
    val redisConnection = new RedisConnect(unifiedPipelineConfig.redisHost, unifiedPipelineConfig.redisPort, unifiedPipelineConfig.redisConnectionTimeout)
    redisConnection.getConnection(config.getInt("redis.database.extractor.duplication.store.id")).flushAll()
    redisConnection.getConnection(config.getInt("redis.database.preprocessor.duplication.store.id")).flushAll()
    super.afterAll()
    flinkCluster.after()
    EmbeddedKafka.stop()
  }

  def createTestTopics(): Unit = {
    List(
      config.getString("kafka.output.system.event.topic"), config.getString("kafka.output.transform.topic"), config.getString("kafka.output.denorm.failed.topic"),
      config.getString("kafka.output.denorm.topic"), config.getString("kafka.output.duplicate.topic"), config.getString("kafka.output.unique.topic"),
      config.getString("kafka.output.invalid.topic"), config.getString("kafka.output.batch.failed.topic"), config.getString("kafka.output.failed.topic"),
      config.getString("kafka.output.extractor.duplicate.topic"), config.getString("kafka.output.raw.topic"), config.getString("kafka.input.topic"),
      "d1-events", "d2-events"
    ).foreach(EmbeddedKafka.createCustomTopic(_))
  }

  "UnifiedPipelineStreamTaskTestSpec" should "validate the entire pipeline" in {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(unifiedPipelineConfig)
    val task = new UnifiedPipelineStreamTask(config, unifiedPipelineConfig, kafkaConnector)
    task.process(env)
    Future {
      env.execute(unifiedPipelineConfig.jobName)
    }

    try {
      val d1Events = EmbeddedKafka.consumeNumberMessagesFrom[String]("d1-events", 1, timeout = 30.seconds)
      d1Events.size should be(1)
      val d2Events = EmbeddedKafka.consumeNumberMessagesFrom[String]("d2-events", 1, timeout = 30.seconds)
      d2Events.size should be(1)
    } catch {
      case ex: Exception => ex.printStackTrace()
    }
    try {
      val systemEvents = EmbeddedKafka.consumeNumberMessagesFrom[String](config.getString("kafka.output.system.event.topic"), 7, timeout = 30.seconds)
      systemEvents.size should be(7)
    } catch {
      case ex: Exception => ex.printStackTrace()
    }

    val mutableMetricsMap = mutable.Map[String, Long]();
    BaseMetricsReporter.gaugeMetrics.toMap.mapValues(f => f.getValue()).map(f => mutableMetricsMap.put(f._1, f._2))
    Console.println("### UnifiedPipelineStreamTaskTestSpec:metrics ###", JSONUtil.serialize(getPrintableMetrics(mutableMetricsMap)))

    mutableMetricsMap("ExtractorJob.d1.extractor-total-count") should be(4)
    mutableMetricsMap("ExtractorJob.d1.extractor-duplicate-count") should be(1)
    mutableMetricsMap("ExtractorJob.d1.extractor-event-count") should be(1)
    mutableMetricsMap("ExtractorJob.d1.extractor-success-count") should be(1)
    mutableMetricsMap("ExtractorJob.d1.extractor-failed-count") should be(2)
    mutableMetricsMap("ExtractorJob.d2.extractor-total-count") should be(2)
    mutableMetricsMap("ExtractorJob.d2.failed-event-count") should be(1)
    mutableMetricsMap("ExtractorJob.d2.extractor-skipped-count") should be(1)

    mutableMetricsMap("PipelinePreprocessorJob.d1.validator-total-count") should be(1)
    mutableMetricsMap("PipelinePreprocessorJob.d1.validator-success-count") should be(1)
    mutableMetricsMap("PipelinePreprocessorJob.d1.dedup-total-count") should be(1)
    mutableMetricsMap("PipelinePreprocessorJob.d1.dedup-success-count") should be(1)
    mutableMetricsMap("PipelinePreprocessorJob.d2.validator-total-count") should be(1)
    mutableMetricsMap("PipelinePreprocessorJob.d2.validator-skipped-count") should be(1)
    mutableMetricsMap("PipelinePreprocessorJob.d2.dedup-total-count") should be(1)
    mutableMetricsMap("PipelinePreprocessorJob.d2.dedup-skipped-count") should be(1)

    mutableMetricsMap("DenormalizerJob.d1.denorm-total") should be(1)
    mutableMetricsMap("DenormalizerJob.d1.denorm-failed") should be(1)
    mutableMetricsMap("DenormalizerJob.d2.denorm-total") should be(1)
    mutableMetricsMap("DenormalizerJob.d2.denorm-skipped") should be(1)

    mutableMetricsMap("TransformerJob.d1.transform-total-count") should be(1)
    mutableMetricsMap("TransformerJob.d1.transform-success-count") should be(1)
    mutableMetricsMap("TransformerJob.d2.transform-total-count") should be(1)
    mutableMetricsMap("TransformerJob.d2.transform-skipped-count") should be(1)

    mutableMetricsMap("DruidRouterJob.d1.router-total-count") should be(1)
    mutableMetricsMap("DruidRouterJob.d1.router-success-count") should be(1)
    mutableMetricsMap("DruidRouterJob.d2.router-total-count") should be(1)
    mutableMetricsMap("DruidRouterJob.d2.router-success-count") should be(1)

    unifiedPipelineConfig.successTag().getId should be("processing_stats")
    unifiedPipelineConfig.failedEventsOutputTag().getId should be("failed-events")
  }

}
