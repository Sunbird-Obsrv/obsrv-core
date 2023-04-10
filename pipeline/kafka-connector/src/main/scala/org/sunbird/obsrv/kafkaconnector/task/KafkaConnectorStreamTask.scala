package org.sunbird.obsrv.kafkaconnector.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.registry.DatasetRegistry

import java.io.File
import scala.collection.mutable

class KafkaConnectorStreamTask(config: KafkaConnectorConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -7729362727131516112L

  // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val datasetSourceConfig = DatasetRegistry.getDatasetSourceConfig()
    datasetSourceConfig.map { configList =>
      val dataStreamList = configList.filter(_.connectorType.equalsIgnoreCase("kafka")).map {
        dataSourceConfig =>
          val dataStream: DataStream[mutable.Map[String, AnyRef]] =
            getMapDataStream(env, config, List(dataSourceConfig.connectorConfig.topic),
            config.kafkaConsumerProperties(kafkaBrokerServers = Some(dataSourceConfig.connectorConfig.kafkaBrokers),
              kafkaConsumerGroup = Some(s"kafka-${dataSourceConfig.connectorConfig.topic}")),
              s"kafka-${dataSourceConfig.connectorConfig.topic}", kafkaConnector)
          val datasetId = dataSourceConfig.datasetId
          val resultMapStream: DataStream[mutable.Map[String, AnyRef]] = dataStream.map {
            streamMap: mutable.Map[String, AnyRef] => {
              val result: mutable.Map[String, AnyRef] = streamMap + ("datasetId" -> datasetId)
              result
            }
          }.returns(classOf[mutable.Map[String, AnyRef]])
          print(resultMapStream)
          resultMapStream.sinkTo(kafkaConnector.kafkaMapSink(config.kafkaOutputTopic))
            .name(s"${datasetId}-kafka-connector-sink").uid(s"${datasetId}-kafka-connector-sink")
            .setParallelism(config.downstreamOperatorsParallelism)
      }
      env.execute(config.jobName)
    }
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {
    null
  }
  // $COVERAGE-ON$
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
