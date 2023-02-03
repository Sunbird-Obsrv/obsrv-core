package org.sunbird.obsrv.extractor.task

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.extractor.functions.ExtractionFunction

import scala.collection.mutable

/**
 * Extraction stream task to extract batch events if any dependent on dataset configuration
 */
class ExtractorStreamTask(config: ExtractorConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val extractionStream =
      env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic), config.extractorConsumer)
        .uid(config.extractorConsumer).setParallelism(config.kafkaConsumerParallelism)
        .rebalance()
        .process(new ExtractionFunction(config))
        .name(config.extractionFunction).uid(config.extractionFunction)
        .setParallelism(config.downstreamOperatorsParallelism)

    extractionStream.getSideOutput(config.failedBatchEventOutputTag).addSink(kafkaConnector.kafkaMapSink(config.kafkaBatchFailedTopic))
      .name(config.extractorBatchFailedEventsProducer).uid(config.extractorBatchFailedEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractionStream.getSideOutput(config.rawEventsOutputTag).addSink(kafkaConnector.kafkaMapSink(config.kafkaSuccessTopic))
      .name(config.extractorRawEventsProducer).uid(config.extractorRawEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractionStream.getSideOutput(config.duplicateEventOutputTag).addSink(kafkaConnector.kafkaMapSink(config.kafkaDuplicateTopic))
      .name(config.extractorDuplicateProducer).uid(config.extractorDuplicateProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractionStream.getSideOutput(config.systemEventsOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaSystemTopic))
      .name(config.systemEventsProducer).uid(config.systemEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractionStream.getSideOutput(config.failedEventsOutputTag).addSink(kafkaConnector.kafkaMapSink(config.kafkaFailedTopic))
      .name(config.extractorFailedEventsProducer).uid(config.extractorFailedEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object ExtractorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("extractor.conf").withFallback(ConfigFactory.systemEnvironment()))
    val extractorConfig = new ExtractorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(extractorConfig)
    val task = new ExtractorStreamTask(extractorConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
