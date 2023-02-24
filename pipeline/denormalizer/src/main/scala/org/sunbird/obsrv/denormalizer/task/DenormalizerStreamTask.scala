package org.sunbird.obsrv.denormalizer.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.denormalizer.functions.DenormalizerFunction

import java.io.File
import scala.collection.mutable

/**
 * Denormalization stream task does the following pipeline processing in a sequence:
 */
class DenormalizerStreamTask(config: DenormalizerConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    val dataStream = getMapDataStream(env, config, kafkaConnector)
    processStream(dataStream)
    env.execute(config.jobName)
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {
    val denormStream = dataStream
      .process(new DenormalizerFunction(config)).name(config.denormalizationFunction).uid(config.denormalizationFunction)
      .setParallelism(config.downstreamOperatorsParallelism)

    denormStream.getSideOutput(config.denormEventsTag).sinkTo(kafkaConnector.kafkaMapSink(config.denormOutputTopic))
      .name(config.DENORM_EVENTS_PRODUCER).uid(config.DENORM_EVENTS_PRODUCER).setParallelism(config.downstreamOperatorsParallelism)
    denormStream.getSideOutput(config.denormFailedTag).sinkTo(kafkaConnector.kafkaMapSink(config.denormFailedTopic))
      .name(config.DENORM_FAILED_EVENTS_PRODUCER).uid(config.DENORM_FAILED_EVENTS_PRODUCER).setParallelism(config.downstreamOperatorsParallelism)

    denormStream.getSideOutput(config.successTag())
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object DenormalizerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("de-normalization.conf").withFallback(ConfigFactory.systemEnvironment()))
    val denormalizationConfig = new DenormalizerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(denormalizationConfig)
    val task = new DenormalizerStreamTask(denormalizationConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$