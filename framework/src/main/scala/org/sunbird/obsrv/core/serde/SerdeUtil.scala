package org.sunbird.obsrv.core.serde

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.sunbird.obsrv.core.model.Constants
import org.sunbird.obsrv.core.util.JSONUtil

import java.nio.charset.StandardCharsets
import scala.collection.mutable


class MapDeserializationSchema extends KafkaRecordDeserializationSchema[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -3224825136576915426L

  override def getProducedType: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[mutable.Map[String, AnyRef]]): Unit = {
    val msg = try {
      JSONUtil.deserialize[mutable.Map[String, AnyRef]](record.value())
    } catch {
      case _: Exception =>
        mutable.Map[String, AnyRef](Constants.INVALID_JSON -> new String(record.value, "UTF-8"))
    }
    initObsrvMeta(msg, record)
    out.collect(msg)
  }

  private def initObsrvMeta(msg: mutable.Map[String, AnyRef], record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    if (!msg.contains("obsrv_meta")) {
      msg.put("obsrv_meta", Map(
        "syncts" -> record.timestamp(),
        "processingStartTime" -> System.currentTimeMillis(),
        "flags" -> Map(),
        "timespans" -> Map(),
        "error" -> Map()
      ))
    }
  }

}

class StringDeserializationSchema extends KafkaRecordDeserializationSchema[String] {

  private val serialVersionUID = -3224825136576915426L

  override def getProducedType: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[String]): Unit = {
    out.collect(new String(record.value(), StandardCharsets.UTF_8))
  }
}

class SerializationSchema[T](topic: String) extends KafkaRecordSerializationSchema[T] {

  private val serialVersionUID = -4284080856874185929L

  override def serialize(element: T, context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val out = JSONUtil.serialize(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, out.getBytes(StandardCharsets.UTF_8))
  }
}

class DynamicMapSerializationSchema() extends KafkaRecordSerializationSchema[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -4284080856874185929L

  override def serialize(element: mutable.Map[String, AnyRef], context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val out = JSONUtil.serialize(element.get(Constants.MESSAGE))
    val topic = element.get(Constants.TOPIC).get.asInstanceOf[String]
    new ProducerRecord[Array[Byte], Array[Byte]](topic, out.getBytes(StandardCharsets.UTF_8))
  }
}