package org.sunbird.obsrv.preprocessor.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.preprocessor.functions.{DeduplicationFunction, EventValidationFunction}

import java.io.File
import scala.collection.mutable

class PipelinePreprocessorStreamTask(config: PipelinePreprocessorConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = 146697324640926024L
  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    val dataStream = getMapDataStream(env, config, kafkaConnector)
    processStream(dataStream)
    env.execute(config.jobName)
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {

    val validStream = dataStream.process(new EventValidationFunction(config)).setParallelism(config.downstreamOperatorsParallelism)
    val uniqueStream = validStream.getSideOutput(config.validEventsOutputTag)
      .process(new DeduplicationFunction(config))
      .name(config.dedupConsumer).uid(config.dedupConsumer)
      .setParallelism(config.downstreamOperatorsParallelism)

    /**
     * Sink for invalid events, duplicate events and system events
     */
    validStream.getSideOutput(config.failedEventsOutputTag).addSink(kafkaConnector.kafkaMapSink(config.kafkaFailedTopic))
      .name(config.failedEventProducer).uid(config.failedEventProducer).setParallelism(config.downstreamOperatorsParallelism)
    validStream.getSideOutput(config.systemEventsOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaSystemTopic))
      .name(config.validationConsumer + "-" + config.systemEventsProducer).uid(config.validationConsumer + "-" + config.systemEventsProducer).setParallelism(config.downstreamOperatorsParallelism)
    validStream.getSideOutput(config.invalidEventsOutputTag).addSink(kafkaConnector.kafkaMapSink(config.kafkaInvalidTopic))
      .name(config.invalidEventProducer).uid(config.invalidEventProducer).setParallelism(config.downstreamOperatorsParallelism)

    uniqueStream.getSideOutput(config.duplicateEventsOutputTag).addSink(kafkaConnector.kafkaMapSink(config.kafkaDuplicateTopic))
      .name(config.duplicateEventProducer).uid(config.duplicateEventProducer).setParallelism(config.downstreamOperatorsParallelism)
    uniqueStream.getSideOutput(config.systemEventsOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaSystemTopic))
      .name(config.dedupConsumer + "-" + config.systemEventsProducer).uid(config.dedupConsumer + "-" + config.systemEventsProducer).setParallelism(config.downstreamOperatorsParallelism)
    uniqueStream.getSideOutput(config.successTag()).addSink(kafkaConnector.kafkaMapSink(config.kafkaUniqueTopic))
      .name(config.uniqueEventProducer).uid(config.uniqueEventProducer).setParallelism(config.downstreamOperatorsParallelism)

    uniqueStream.getSideOutput(config.successTag())
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object PipelinePreprocessorStreamTask {
  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("pipeline-preprocessor.conf").withFallback(ConfigFactory.systemEnvironment()))
    val pipelinePreprocessorConfig = new PipelinePreprocessorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(pipelinePreprocessorConfig)
    val task = new PipelinePreprocessorStreamTask(pipelinePreprocessorConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$