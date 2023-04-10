package org.sunbird.obsrv.core.streaming


import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.util.Properties
import scala.collection.mutable

abstract class BaseStreamTask[T] {

  def process()

  def processStream(dataStream: DataStream[T]): DataStream[T]

  def getMapDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStream[mutable.Map[String, AnyRef]] = {
    env.fromSource(kafkaConnector.kafkaMapSource(config.inputTopic()), WatermarkStrategy.noWatermarks[mutable.Map[String, AnyRef]](), config.inputConsumer())
      .uid(config.inputConsumer()).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

  def getMapDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaTopics: List[String],
                       kafkaConsumerProperties: Properties, consumerSourceName: String, kafkaConnector: FlinkKafkaConnector): DataStream[mutable.Map[String, AnyRef]] = {
    env.fromSource(kafkaConnector.kafkaMapSource(kafkaTopics, kafkaConsumerProperties), WatermarkStrategy.noWatermarks[mutable.Map[String, AnyRef]](), consumerSourceName)
      .uid(consumerSourceName).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

  def getStringDataStream(env: StreamExecutionEnvironment, config: BaseJobConfig[T], kafkaConnector: FlinkKafkaConnector): DataStream[String] = {
    env.fromSource(kafkaConnector.kafkaStringSource(config.inputTopic()), WatermarkStrategy.noWatermarks[String](), config.inputConsumer())
      .uid(config.inputConsumer()).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
  }

}
