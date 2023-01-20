package org.sunbird.obsrv.core.streaming

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.sunbird.obsrv.core.serde._

import scala.collection.mutable

class FlinkKafkaConnector(config: BaseJobConfig) extends Serializable {

  def kafkaMapSource(kafkaTopic: String): SourceFunction[mutable.Map[String, AnyRef]] = {
    new FlinkKafkaConsumer[mutable.Map[String, AnyRef]](kafkaTopic, new MapDeserializationSchema, config.kafkaConsumerProperties)
  }

  def kafkaMapSink(kafkaTopic: String): SinkFunction[mutable.Map[String, AnyRef]] = {
    new FlinkKafkaProducer[mutable.Map[String, AnyRef]](kafkaTopic, new MapSerializationSchema(kafkaTopic), config.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

  def kafkaStringSource(kafkaTopic: String): SourceFunction[String] = {
    new FlinkKafkaConsumer[String](kafkaTopic, new StringDeserializationSchema, config.kafkaConsumerProperties)
  }

  def kafkaStringSink(kafkaTopic: String): SinkFunction[String] = {
    new FlinkKafkaProducer[String](kafkaTopic, new StringSerializationSchema(kafkaTopic), config.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

}