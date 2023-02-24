package org.sunbird.obsrv.core.streaming

import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.sunbird.obsrv.core.serde._

import scala.collection.mutable

class FlinkKafkaConnector(config: BaseJobConfig[_]) extends Serializable {

  def kafkaStringSource(kafkaTopic: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setTopics(kafkaTopic)
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties)
      .build()
  }

  def kafkaStringSink(kafkaTopic: String): KafkaSink[String] = {
    KafkaSink.builder[String]()
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new StringSerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }

  def kafkaMapSource(kafkaTopic: String): KafkaSource[mutable.Map[String, AnyRef]] = {
    KafkaSource.builder[mutable.Map[String, AnyRef]]()
      .setTopics(kafkaTopic)
      .setDeserializer(new MapDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties)
      .build()
  }

  def kafkaMapSink(kafkaTopic: String): KafkaSink[mutable.Map[String, AnyRef]] = {
    KafkaSink.builder[mutable.Map[String, AnyRef]]()
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new MapSerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }

}