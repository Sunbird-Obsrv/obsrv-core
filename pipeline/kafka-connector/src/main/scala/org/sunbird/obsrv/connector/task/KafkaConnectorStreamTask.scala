package org.sunbird.obsrv.connector.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil}
import org.sunbird.obsrv.registry.DatasetRegistry

import java.io.File

class KafkaConnectorStreamTask(config: KafkaConnectorConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[String] {

  private val serialVersionUID = -7729362727131516112L

  // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    env.execute(config.jobName)
  }

  override def processStream(dataStream: DataStream[String]): DataStream[String] = {
    null
  }
  // $COVERAGE-ON$

  def process(env: StreamExecutionEnvironment): Unit = {
    val datasetSourceConfig = DatasetRegistry.getAllDatasetSourceConfig()
    datasetSourceConfig.map { configList =>
      configList.filter(_.connectorType.equalsIgnoreCase("kafka")).map {
        dataSourceConfig =>
          val dataStream: DataStream[String] = getStringDataStream(env, config, List(dataSourceConfig.connectorConfig.topic),
            config.kafkaConsumerProperties(kafkaBrokerServers = Some(dataSourceConfig.connectorConfig.kafkaBrokers),
              kafkaConsumerGroup = Some(s"kafka-${dataSourceConfig.connectorConfig.topic}-consumer")),
            consumerSourceName = s"kafka-${dataSourceConfig.connectorConfig.topic}", kafkaConnector)
          val datasetId = dataSourceConfig.datasetId
          val kafkaOutputTopic = DatasetRegistry.getDataset(datasetId).get.datasetConfig.entryTopic
          val resultStream: DataStream[String] = dataStream.map { streamData: String => {
            val syncts = java.lang.Long.valueOf(new DateTime(DateTimeZone.UTC).getMillis)
            JSONUtil.getJsonType(streamData) match {
              case "ARRAY" => s"""{"dataset":"$datasetId","syncts":$syncts,"events":$streamData}"""
              case _ => s"""{"dataset":"$datasetId","syncts":$syncts,"event":$streamData}"""
            }
          }
          }.returns(classOf[String])
          resultStream.sinkTo(kafkaConnector.kafkaSink[String](kafkaOutputTopic))
            .name(s"$datasetId-kafka-connector-sink").uid(s"$datasetId-kafka-connector-sink")
            .setParallelism(config.downstreamOperatorsParallelism)
      }
    }
  }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object KafkaConnectorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("kafka-connector.conf").withFallback(ConfigFactory.systemEnvironment()))
    val kafkaConnectorConfig = new KafkaConnectorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(kafkaConnectorConfig)
    val task = new KafkaConnectorStreamTask(kafkaConnectorConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$