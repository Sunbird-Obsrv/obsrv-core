package org.sunbird.spec

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil

class TestStringStreamTask(config: BaseProcessTestConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[String] {
  override def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    val dataStream = getStringDataStream(env, config, kafkaConnector)
    processStream(dataStream)
    val dataStream2 = getStringDataStream(env, config, List(config.inputTopic()), config.kafkaConsumerProperties(), config.inputConsumer(), kafkaConnector)
    env.execute(config.jobName)
  }

  override def processStream(dataStream: DataStream[String]): DataStream[String] = {
    val stream = dataStream.process(new TestStringStreamFunc(config)).name("TestStringEventStream")
    stream.getSideOutput(config.stringOutputTag)
      .sinkTo(kafkaConnector.kafkaSink[String](config.kafkaStringOutputTopic))
      .name("String-Event-Producer")

    stream.getSideOutput(config.stringOutputTag)
  }
}
