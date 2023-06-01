package org.sunbird.spec

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.FlinkUtil

import scala.collection.mutable

class TestMapStreamTask(config: BaseProcessTestMapConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[mutable.Map[String, AnyRef]] {
  override def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    val dataStream = getMapDataStream(env, config, kafkaConnector)
    processStream(dataStream)
    env.execute(config.jobName)
  }

  override def processStream(dataStream: DataStream[mutable.Map[String, AnyRef]]): DataStream[mutable.Map[String, AnyRef]] = {
    val stream = dataStream.process(new TestMapStreamFunc(config))
    stream.getSideOutput(config.mapOutputTag)
      .sinkTo(kafkaConnector.kafkaMapSink(config.kafkaMapOutputTopic))
      .name("Map-Event-Producer")

    stream.getSideOutput(config.mapOutputTag)
  }
}
