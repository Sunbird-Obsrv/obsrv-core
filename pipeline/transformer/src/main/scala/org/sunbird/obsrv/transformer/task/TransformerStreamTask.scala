package org.sunbird.obsrv.transformer.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.transformer.functions.TransformerFunction

import java.io.File
import scala.collection.mutable

/**
 *
 */
class TransformerStreamTask(config: TransformerConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -7729362727131516112L
  implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    val dataStream = getMapDataStream(env, config, kafkaConnector)
    processStream(dataStream)
    env.execute(config.jobName)
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {
    val transformedStream = dataStream.process(new TransformerFunction(config)).name(config.transformerFunction).uid(config.transformerFunction)
      .setParallelism(config.downstreamOperatorsParallelism)

    transformedStream.getSideOutput(config.transformerOutputTag)
      .addSink(kafkaConnector.kafkaMapSink(config.kafkaTransformTopic))
      .name(config.transformerProducer).uid(config.transformerProducer)
      .setParallelism(config.downstreamOperatorsParallelism)
    transformedStream.getSideOutput(config.successTag())
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object TransformerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("transformer.conf").withFallback(ConfigFactory.systemEnvironment()))
    val telemetryExtractorConfig = new TransformerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(telemetryExtractorConfig)
    val task = new TransformerStreamTask(telemetryExtractorConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
