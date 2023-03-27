package org.sunbird.spec

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil

import scala.collection.mutable

class TestStringStreamTask(config: BaseProcessTestConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[String] {
  override def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    val dataStream = getStringDataStream(env, config, kafkaConnector)
    processStream(dataStream)
    env.execute(config.jobName)
  }

  override def processStream(dataStream: DataStream[String]): DataStream[String] = {
    val stream = dataStream.process(new TestStringStreamFunc(config)).name("TestStringEventStream")
    stream.getSideOutput(config.stringOutputTag)
      .sinkTo(kafkaConnector.kafkaStringSink(config.kafkaStringOutputTopic))
      .name("String-Event-Producer")

    stream.getSideOutput(config.stringOutputTag)
  }
}
