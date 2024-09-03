package org.sunbird.obsrv.streaming

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.function.MasterDataProcessorFunction
import org.sunbird.obsrv.model.DatasetType
import org.sunbird.obsrv.pipeline.task.CacheIndexerConfig
import org.sunbird.obsrv.registry.DatasetRegistry

import java.io.File
import scala.collection.mutable

class CacheIndexerStreamTask(config: CacheIndexerConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  implicit val mutableMapTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  private val logger = LoggerFactory.getLogger(classOf[CacheIndexerStreamTask])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    process(env)
    env.execute(config.jobName)
  }

  def process(env: StreamExecutionEnvironment): Unit = {

    val datasets = DatasetRegistry.getAllDatasets(Some(DatasetType.master.toString))
    val datasetIds = datasets.map(f => f.id)
    val dataStream = getTopicMapDataStream(env, config, datasetIds, consumerSourceName = s"cache-indexer-consumer", kafkaConnector)
    processStream(dataStream)
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {
    val processedStream = dataStream.process(new MasterDataProcessorFunction(config)).name(config.cacheIndexerFunction)
      .uid(config.cacheIndexerFunction).setParallelism(config.downstreamOperatorsParallelism)
    addDefaultSinks(processedStream, config, kafkaConnector)
    processedStream.getSideOutput(config.successTag())
  }

}

object CacheIndexerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("cache-indexer.conf").withFallback(ConfigFactory.systemEnvironment()))
    val cacheConfig = new CacheIndexerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(cacheConfig)
    val task = new CacheIndexerStreamTask(cacheConfig, kafkaUtil)
    task.process()
  }

}