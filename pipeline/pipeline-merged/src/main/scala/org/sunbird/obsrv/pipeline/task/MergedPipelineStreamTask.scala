package org.sunbird.obsrv.pipeline.task

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.denormalizer.functions.DenormalizerFunction
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.extractor.functions.ExtractionFunction
import org.sunbird.obsrv.extractor.task.ExtractorConfig
import org.sunbird.obsrv.preprocessor.functions.{DeduplicationFunction, EventValidationFunction}
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.router.functions.DruidRouterFunction
import org.sunbird.obsrv.router.task.DruidRouterConfig
import org.sunbird.obsrv.transformer.functions.TransformerFunction
import org.sunbird.obsrv.transformer.task.TransformerConfig

import java.io.File
import scala.collection.mutable

/**
 * Druid Router stream task routes every event into its respective topic configured at dataset level
 */

class MergedPipelineStreamTask(config: Config, mergedPipelineConfig: MergedPipelineConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = 146697324640926024L

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(mergedPipelineConfig)
    implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val extractorConfig = new ExtractorConfig(config)
    val preprocessorConfig = new PipelinePreprocessorConfig(config)
    val denormalizerConfig = new DenormalizerConfig(config)
    val transformerConfig = new TransformerConfig(config)
    val routerConfig = new DruidRouterConfig(config)

    val datasets = DatasetRegistry.getAllDatasets()

    /** Create a data stream from the input topic */
    val dataStream = env.fromSource(kafkaConnector.kafkaMapSource(mergedPipelineConfig.kafkaInputTopic),
      WatermarkStrategy.noWatermarks[mutable.Map[String, AnyRef]](), mergedPipelineConfig.pipelineConsumer)
      .uid(mergedPipelineConfig.pipelineConsumer).setParallelism(mergedPipelineConfig.kafkaConsumerParallelism)
      .rebalance()

    /** Success Flow - Start */
    val extractorStream = dataStream
      .process(new ExtractionFunction(extractorConfig)).name(extractorConfig.extractionFunction).uid(extractorConfig.extractionFunction)
      .setParallelism(extractorConfig.downstreamOperatorsParallelism)

    val validStream = extractorStream.getSideOutput(extractorConfig.rawEventsOutputTag)
      .process(new EventValidationFunction(preprocessorConfig)).setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    val uniqueStream = validStream.getSideOutput(preprocessorConfig.validEventsOutputTag)
      .process(new DeduplicationFunction(preprocessorConfig)).name(preprocessorConfig.dedupConsumer).uid(preprocessorConfig.dedupConsumer)
      .setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    val denormStream = uniqueStream.getSideOutput(preprocessorConfig.uniqueEventsOutputTag)
      .process(new DenormalizerFunction(denormalizerConfig)).name(denormalizerConfig.denormalizationFunction).uid(denormalizerConfig.denormalizationFunction)
      .setParallelism(denormalizerConfig.downstreamOperatorsParallelism)

    val transformedStream = denormStream.getSideOutput(denormalizerConfig.denormEventsTag)
      .process(new TransformerFunction(transformerConfig)).name(transformerConfig.transformerFunction).uid(transformerConfig.transformerFunction)
      .setParallelism(transformerConfig.downstreamOperatorsParallelism)

    val routerStream = transformedStream.getSideOutput(transformerConfig.transformerOutputTag)
      .process(new DruidRouterFunction(routerConfig)).name(routerConfig.druidRouterFunction).uid(routerConfig.druidRouterFunction)
      .setParallelism(routerConfig.downstreamOperatorsParallelism)

    datasets.map(dataset => {
      routerStream.getSideOutput(OutputTag[mutable.Map[String, AnyRef]](dataset.routerConfig.topic))
        .sinkTo(kafkaConnector.kafkaMapSink(dataset.routerConfig.topic))
        .name(dataset.id + "-" + routerConfig.druidRouterProducer).uid(dataset.id + "-" + routerConfig.druidRouterProducer)
        .setParallelism(routerConfig.downstreamOperatorsParallelism)
    })

    /** Success Flow - End */

    /** Exception Flow - Start */
    extractorStream.getSideOutput(extractorConfig.failedBatchEventOutputTag).sinkTo(kafkaConnector.kafkaMapSink(extractorConfig.kafkaBatchFailedTopic))
      .name(extractorConfig.extractorBatchFailedEventsProducer).uid(extractorConfig.extractorBatchFailedEventsProducer).setParallelism(extractorConfig.downstreamOperatorsParallelism)
    extractorStream.getSideOutput(extractorConfig.duplicateEventOutputTag).sinkTo(kafkaConnector.kafkaMapSink(extractorConfig.kafkaDuplicateTopic))
      .name(extractorConfig.extractorDuplicateProducer).uid(extractorConfig.extractorDuplicateProducer).setParallelism(extractorConfig.downstreamOperatorsParallelism)
    extractorStream.getSideOutput(extractorConfig.systemEventsOutputTag).sinkTo(kafkaConnector.kafkaStringSink(extractorConfig.kafkaSystemTopic))
      .name(extractorConfig.systemEventsProducer).uid(extractorConfig.systemEventsProducer).setParallelism(extractorConfig.downstreamOperatorsParallelism)
    extractorStream.getSideOutput(extractorConfig.failedEventsOutputTag).sinkTo(kafkaConnector.kafkaMapSink(extractorConfig.kafkaFailedTopic))
      .name(extractorConfig.extractorFailedEventsProducer).uid(extractorConfig.extractorFailedEventsProducer).setParallelism(extractorConfig.downstreamOperatorsParallelism)

    validStream.getSideOutput(preprocessorConfig.failedEventsOutputTag).sinkTo(kafkaConnector.kafkaMapSink(preprocessorConfig.kafkaFailedTopic))
      .name(preprocessorConfig.failedEventProducer).uid(preprocessorConfig.failedEventProducer).setParallelism(preprocessorConfig.downstreamOperatorsParallelism)
    validStream.getSideOutput(preprocessorConfig.systemEventsOutputTag).sinkTo(kafkaConnector.kafkaStringSink(preprocessorConfig.kafkaSystemTopic))
      .name(preprocessorConfig.validationConsumer + "-" + preprocessorConfig.systemEventsProducer).uid(preprocessorConfig.validationConsumer + "-" + preprocessorConfig.systemEventsProducer).setParallelism(preprocessorConfig.downstreamOperatorsParallelism)
    validStream.getSideOutput(preprocessorConfig.invalidEventsOutputTag).sinkTo(kafkaConnector.kafkaMapSink(preprocessorConfig.kafkaInvalidTopic))
      .name(preprocessorConfig.invalidEventProducer).uid(preprocessorConfig.invalidEventProducer).setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    uniqueStream.getSideOutput(preprocessorConfig.duplicateEventsOutputTag).sinkTo(kafkaConnector.kafkaMapSink(preprocessorConfig.kafkaDuplicateTopic))
      .name(preprocessorConfig.duplicateEventProducer).uid(preprocessorConfig.duplicateEventProducer).setParallelism(preprocessorConfig.downstreamOperatorsParallelism)
    uniqueStream.getSideOutput(preprocessorConfig.systemEventsOutputTag).sinkTo(kafkaConnector.kafkaStringSink(preprocessorConfig.kafkaSystemTopic))
      .name(preprocessorConfig.dedupConsumer + "-" + preprocessorConfig.systemEventsProducer).uid(preprocessorConfig.dedupConsumer + "-" + preprocessorConfig.systemEventsProducer).setParallelism(preprocessorConfig.downstreamOperatorsParallelism)

    denormStream.getSideOutput(denormalizerConfig.denormFailedTag).sinkTo(kafkaConnector.kafkaMapSink(denormalizerConfig.denormFailedTopic))
      .name(denormalizerConfig.DENORM_FAILED_EVENTS_PRODUCER).uid(denormalizerConfig.DENORM_FAILED_EVENTS_PRODUCER).setParallelism(denormalizerConfig.downstreamOperatorsParallelism)

    routerStream.getSideOutput(routerConfig.statsOutputTag).sinkTo(kafkaConnector.kafkaMapSink(routerConfig.kafkaStatsTopic))
      .name(routerConfig.processingStatsProducer).uid(routerConfig.processingStatsProducer).setParallelism(routerConfig.downstreamOperatorsParallelism)

    /** Exception Flow - End */

    env.execute(mergedPipelineConfig.jobName)
  }

}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object MergedPipelineStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("merged-pipeline.conf").withFallback(ConfigFactory.systemEnvironment()))
    val mergedPipelineConfig = new MergedPipelineConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(mergedPipelineConfig)
    val task = new MergedPipelineStreamTask(config, mergedPipelineConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$