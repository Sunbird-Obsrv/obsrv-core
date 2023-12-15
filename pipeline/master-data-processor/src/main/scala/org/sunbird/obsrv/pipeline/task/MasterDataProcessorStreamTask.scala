package org.sunbird.obsrv.pipeline.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.{DatasetKeySelector, FlinkUtil, TumblingProcessingTimeCountWindows}
import org.sunbird.obsrv.denormalizer.task.{DenormalizerConfig, DenormalizerStreamTask}
import org.sunbird.obsrv.extractor.task.{ExtractorConfig, ExtractorStreamTask}
import org.sunbird.obsrv.pipeline.function.MasterDataProcessorFunction
import org.sunbird.obsrv.preprocessor.task.{PipelinePreprocessorConfig, PipelinePreprocessorStreamTask}
import org.sunbird.obsrv.transformer.task.{TransformerConfig, TransformerStreamTask}

import java.io.File
import scala.collection.mutable

/**
 * Master data processor task to process master data and cache it in redis for denormalization
 */

class MasterDataProcessorStreamTask(config: Config, masterDataConfig: MasterDataProcessorConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = 146697324640926024L

  // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(masterDataConfig)
    process(env)
    env.execute(masterDataConfig.jobName)
  }
  // $COVERAGE-ON$

  /**
   * Created an overloaded process function to enable unit testing
   *
   * @param env StreamExecutionEnvironment
   */
  def process(env: StreamExecutionEnvironment): Unit = {

    val dataStream = getMapDataStream(env, masterDataConfig, kafkaConnector)
    processStream(dataStream)
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {

    val extractorTask = new ExtractorStreamTask(new ExtractorConfig(config), kafkaConnector)
    val preprocessorTask = new PipelinePreprocessorStreamTask(new PipelinePreprocessorConfig(config), kafkaConnector)
    val denormalizerTask = new DenormalizerStreamTask(new DenormalizerConfig(config), kafkaConnector)
    val transformerTask = new TransformerStreamTask(new TransformerConfig(config), kafkaConnector)

    val transformedStream = transformerTask.processStream(
      denormalizerTask.processStream(
        preprocessorTask.processStream(
          extractorTask.processStream(dataStream)
        )
      )
    )

    val windowedStream = transformedStream.keyBy(new DatasetKeySelector())
      .window(TumblingProcessingTimeCountWindows.of(Time.seconds(masterDataConfig.windowTime), masterDataConfig.windowCount))

    val processedStream = windowedStream.process(new MasterDataProcessorFunction(masterDataConfig)).name(masterDataConfig.masterDataProcessFunction)
      .uid(masterDataConfig.masterDataProcessFunction).setParallelism(masterDataConfig.downstreamOperatorsParallelism)

    addDefaultSinks(processedStream, masterDataConfig, kafkaConnector)
    processedStream.getSideOutput(masterDataConfig.successTag())
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object MasterDataProcessorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("master-data-processor.conf").withFallback(ConfigFactory.systemEnvironment()))
    val masterDataConfig = new MasterDataProcessorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(masterDataConfig)
    val task = new MasterDataProcessorStreamTask(config, masterDataConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$