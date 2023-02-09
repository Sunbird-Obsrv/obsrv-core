package org.sunbird.obsrv.denormalizer.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.WindowedStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.sunbird.obsrv.core.streaming.FlinkKafkaConnector
import org.sunbird.obsrv.core.util.FlinkUtil
import org.sunbird.obsrv.denormalizer.functions.DenormalizerWindowFunction
import org.sunbird.obsrv.denormalizer.util.TumblingProcessingTimeCountWindows

import java.io.File
import scala.collection.mutable

/**
 * Denormalization stream task does the following pipeline processing in a sequence:
 */
class DenormalizerStreamTask(config: DenormalizerConfig, kafkaConnector: FlinkKafkaConnector) {

  private val serialVersionUID = -7729362727131516112L

  def process(): Unit = {

    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

    val source: SourceFunction[mutable.Map[String, AnyRef]] = kafkaConnector.kafkaMapSource(config.inputTopic)
    val windowedStream: WindowedStream[mutable.Map[String, AnyRef], String, TimeWindow] = env.addSource(source, config.denormalizationConsumer).uid(config.denormalizationConsumer)
      .setParallelism(config.kafkaConsumerParallelism).rebalance()
      .keyBy(new DenormKeySelector())
      .window(TumblingProcessingTimeCountWindows.of(Time.seconds(config.windowTime), config.windowCount))

    val denormStream = windowedStream
        .process(new DenormalizerWindowFunction(config)).name(config.denormalizationFunction).uid(config.denormalizationFunction)
        .setParallelism(config.downstreamOperatorsParallelism)

    denormStream.getSideOutput(config.denormEventsTag).addSink(kafkaConnector.kafkaMapSink(config.denormOutputTopic))
      .name(config.DENORM_EVENTS_PRODUCER).uid(config.DENORM_EVENTS_PRODUCER).setParallelism(config.downstreamOperatorsParallelism)
    denormStream.getSideOutput(config.denormFailedTag).addSink(kafkaConnector.kafkaMapSink(config.denormFailedTopic))
      .name(config.DENORM_FAILED_EVENTS_PRODUCER).uid(config.DENORM_FAILED_EVENTS_PRODUCER).setParallelism(config.downstreamOperatorsParallelism)

    env.execute(config.jobName)
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

class DenormKeySelector() extends KeySelector[mutable.Map[String, AnyRef], String] {

  override def getKey(in: mutable.Map[String, AnyRef]): String = {
    in("dataset").asInstanceOf[String]
  }
}