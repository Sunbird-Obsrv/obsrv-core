package org.sunbird.obsrv.extractor.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.extractor.functions.ExtractionFunction

import java.io.File
import scala.collection.mutable

/**
 * Extraction stream task to extract batch events if any dependent on dataset configuration
 */
class ExtractorStreamTask(config: ExtractorConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -7729362727131516112L

  // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    val dataStream = getMapDataStream(env, config, kafkaConnector)
    processStream(dataStream)
    env.execute(config.jobName)
  }
  // $COVERAGE-ON$

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {

    val extractorStream = dataStream.process(new ExtractionFunction(config))
      .name(config.extractionFunction).uid(config.extractionFunction)
      .setParallelism(config.downstreamOperatorsParallelism)

    extractorStream.getSideOutput(config.failedBatchEventOutputTag).sinkTo(kafkaConnector.kafkaMapSink(config.kafkaBatchFailedTopic))
      .name(config.extractorBatchFailedEventsProducer).uid(config.extractorBatchFailedEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractorStream.getSideOutput(config.successTag()).sinkTo(kafkaConnector.kafkaMapSink(config.kafkaSuccessTopic))
      .name(config.extractorRawEventsProducer).uid(config.extractorRawEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractorStream.getSideOutput(config.duplicateEventOutputTag).sinkTo(kafkaConnector.kafkaMapSink(config.kafkaDuplicateTopic))
      .name(config.extractorDuplicateProducer).uid(config.extractorDuplicateProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractorStream.getSideOutput(config.systemEventsOutputTag).sinkTo(kafkaConnector.kafkaStringSink(config.kafkaSystemTopic))
      .name(config.systemEventsProducer).uid(config.systemEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractorStream.getSideOutput(config.failedEventsOutputTag).sinkTo(kafkaConnector.kafkaMapSink(config.kafkaFailedTopic))
      .name(config.extractorFailedEventsProducer).uid(config.extractorFailedEventsProducer).setParallelism(config.downstreamOperatorsParallelism)

    extractorStream.getSideOutput(config.successTag())
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