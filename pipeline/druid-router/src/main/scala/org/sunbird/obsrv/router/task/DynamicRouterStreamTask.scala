package org.sunbird.obsrv.router.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.router.functions.DynamicRouterFunction

import java.io.File
import scala.collection.mutable

/**
 * Druid Router stream task routes every event into its respective topic configured at dataset level
 */

class DynamicRouterStreamTask(config: DruidRouterConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = 146697324640926024L

  // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    process(env)
    env.execute(config.jobName)
  }
  // $COVERAGE-ON$

  def process(env: StreamExecutionEnvironment): Unit = {
    val dataStream = getMapDataStream(env, config, kafkaConnector)
    processStream(dataStream)
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {

    implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

    val routerStream = dataStream.process(new DynamicRouterFunction(config)).name(config.druidRouterFunction).uid(config.druidRouterFunction)
      .setParallelism(config.downstreamOperatorsParallelism)

    routerStream.getSideOutput(config.routerOutputTag).sinkTo(kafkaConnector.kafkaMapDynamicSink())
      .name(config.druidRouterProducer).uid(config.druidRouterProducer).setParallelism(config.downstreamOperatorsParallelism)

    addDefaultSinks(routerStream, config, kafkaConnector)
    routerStream.getSideOutput(config.successTag())
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object DynamicRouterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("druid-router.conf").withFallback(ConfigFactory.systemEnvironment()))
    val druidRouterConfig = new DruidRouterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(druidRouterConfig)
    val task = new DynamicRouterStreamTask(druidRouterConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$