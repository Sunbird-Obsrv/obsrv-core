package org.sunbird.obsrv.core.streaming

import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.sunbird.obsrv.core.serde._

import java.util.Properties
import scala.collection.mutable
import scala.collection.JavaConverters._

class FlinkKafkaConnector(config: BaseJobConfig[_]) extends Serializable {

  def kafkaStringSource(kafkaTopic: String): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setTopics(kafkaTopic)
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(config.kafkaConsumerProperties())
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

  def kafkaStringSource(kafkaTopic: List[String], consumerProperties: Properties): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setTopics(kafkaTopic.asJava)
      .setDeserializer(new StringDeserializationSchema)
      .setProperties(consumerProperties)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
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
      .setProperties(config.kafkaConsumerProperties())
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
      .build()
  }

  def kafkaMapSource(kafkaTopics: List[String], consumerProperties: Properties): KafkaSource[mutable.Map[String, AnyRef]] = {
    KafkaSource.builder[mutable.Map[String, AnyRef]]()
      .setTopics(kafkaTopics.asJava)
      .setDeserializer(new MapDeserializationSchema)
      .setProperties(consumerProperties)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
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