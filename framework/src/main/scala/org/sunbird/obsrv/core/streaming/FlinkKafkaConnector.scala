package org.sunbird.obsrv.core.streaming

import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.sunbird.obsrv.core.serde._

import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable

class FlinkKafkaConnector(config: BaseJobConfig[_]) extends Serializable {

  def kafkaStringSource(kafkaTopic: String): KafkaSource[String] = {
    kafkaStringSource(List(kafkaTopic), config.kafkaConsumerProperties())
  }

  def kafkaStringSource(kafkaTopic: List[String], consumerProperties: Properties): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setTopics(kafkaTopic.asJava)
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(consumerProperties)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

  def kafkaSink[T](kafkaTopic: String): KafkaSink[T] = {
    KafkaSink.builder[T]()
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new SerializationSchema(kafkaTopic))
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }

  def kafkaMapSource(kafkaTopic: String): KafkaSource[mutable.Map[String, AnyRef]] = {
    kafkaMapSource(List(kafkaTopic), config.kafkaConsumerProperties())
  }

  def kafkaMapSource(kafkaTopics: List[String], consumerProperties: Properties): KafkaSource[mutable.Map[String, AnyRef]] = {
    KafkaSource.builder[mutable.Map[String, AnyRef]]()
      .setTopics(kafkaTopics.asJava)
      .setDeserializer(new MapDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties())
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

  def kafkaMapDynamicSink(): KafkaSink[mutable.Map[String, AnyRef]] = {
    KafkaSink.builder[mutable.Map[String, AnyRef]]()
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .setRecordSerializer(new DynamicMapSerializationSchema())
      .setKafkaProducerConfig(config.kafkaProducerProperties)
      .build()
  }

}