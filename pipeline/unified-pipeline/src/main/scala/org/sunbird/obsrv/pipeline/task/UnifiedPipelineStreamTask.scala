package org.sunbird.obsrv.pipeline.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.denormalizer.task.{DenormalizerConfig, DenormalizerStreamTask}
import org.sunbird.obsrv.extractor.task.{ExtractorConfig, ExtractorStreamTask}
import org.sunbird.obsrv.preprocessor.task.{PipelinePreprocessorConfig, PipelinePreprocessorStreamTask}
import org.sunbird.obsrv.router.task.{DynamicRouterConfig, DynamicRouterStreamTask}
import org.sunbird.obsrv.transformer.task.{TransformerConfig, TransformerStreamTask}

import java.io.File
import scala.collection.mutable

/**
 * Druid Router stream task routes every event into its respective topic configured at dataset level
 */

class UnifiedPipelineStreamTask(config: Config, pipelineConfig: UnifiedPipelineConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = 146697324640926024L

  // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(pipelineConfig)
    process(env)
    env.execute(pipelineConfig.jobName)
  }
  // $COVERAGE-ON$

  def process(env: StreamExecutionEnvironment): Unit = {
    val dataStream = getMapDataStream(env, pipelineConfig, kafkaConnector)
    processStream(dataStream)
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {

    val extractorTask = new ExtractorStreamTask(new ExtractorConfig(config), kafkaConnector)
    val preprocessorTask = new PipelinePreprocessorStreamTask(new PipelinePreprocessorConfig(config), kafkaConnector)
    val denormalizerTask = new DenormalizerStreamTask(new DenormalizerConfig(config), kafkaConnector)
    val transformerTask = new TransformerStreamTask(new TransformerConfig(config), kafkaConnector)
    val routerTask = new DynamicRouterStreamTask(new DynamicRouterConfig(config), kafkaConnector)

    routerTask.processStream(
      transformerTask.processStream(
        denormalizerTask.processStream(
          preprocessorTask.processStream(
            extractorTask.processStream(dataStream)
          )
        )
      )
    )
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object UnifiedPipelineStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("unified-pipeline.conf").withFallback(ConfigFactory.systemEnvironment()))
    val pipelineConfig = new UnifiedPipelineConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(pipelineConfig)
    val task = new UnifiedPipelineStreamTask(config, pipelineConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$