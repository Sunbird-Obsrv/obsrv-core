package org.sunbird.obsrv.core.streaming


import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.mutable

abstract class BaseStreamTask[T] {

  def process()

  def processStream(dataStream: DataStream[T]): DataStream[T]

  def getMapDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStream[mutable.Map[String, AnyRef]] = {
    env.addSource(kafkaConnector.kafkaMapSource(config.inputTopic()), config.inputConsumer())
      .uid(config.inputConsumer()).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

  def getStringDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStream[String] = {
    env.addSource(kafkaConnector.kafkaStringSource(config.inputTopic()), config.inputConsumer())
      .uid(config.inputConsumer()).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

}
