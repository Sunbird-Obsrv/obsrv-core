package org.sunbird.obsrv.router.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.router.functions.DruidRouterFunction

import java.io.File
import scala.collection.mutable

/**
 * Druid Router stream task routes every event into its respective topic configured at dataset level
 */

class DruidRouterStreamTask(config: DruidRouterConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = 146697324640926024L

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

    val datasets = DatasetRegistry.getAllDatasets()

    /**
     * Perform validation
     */
    val dataStream =
      env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic), config.druidRouterConsumer)
        .uid(config.druidRouterConsumer).setParallelism(config.kafkaConsumerParallelism)
        .rebalance()
        .process(new DruidRouterFunction(config)).name(config.druidRouterFunction).uid(config.druidRouterFunction)
        .setParallelism(config.downstreamOperatorsParallelism)

    datasets.map(dataset => {
      dataStream.getSideOutput(OutputTag[mutable.Map[String, AnyRef]](dataset.routerConfig.topic))
        .addSink(kafkaConnector.kafkaMapSink(dataset.routerConfig.topic))
        .name(dataset.id + "-" + config.druidRouterProducer).uid(dataset.id + "-" + config.druidRouterProducer)
        .setParallelism(config.downstreamOperatorsParallelism)
    })

    env.execute(config.jobName)
  }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object DruidRouterStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("druid-router.conf").withFallback(ConfigFactory.systemEnvironment()))
    val druidRouterConfig = new DruidRouterConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(druidRouterConfig)
    val task = new DruidRouterStreamTask(druidRouterConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$