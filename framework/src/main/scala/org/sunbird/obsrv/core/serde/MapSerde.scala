package org.sunbird.obsrv.core.serde

import java.nio.charset.StandardCharsets

/*
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.sunbird.obsrv.core.util.JSONUtil
import scala.collection.mutable
*/

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.sunbird.obsrv.core.util.JSONUtil
import scala.collection.mutable


/*
class MapDeserializationSchema extends KafkaDeserializationSchema[mutable.Map[String, AnyRef]] {

  private val serialVersionUID: Long = -3224825136576915426L

  override def isEndOfStream(nextElement: mutable.Map[String, AnyRef]): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): mutable.Map[String, AnyRef] = {

    val msg = JSONUtil.deserialize[mutable.Map[String, AnyRef]](record.value())
    initObsrvMeta(msg, record)
    msg
  }

  private def initObsrvMeta(msg: mutable.Map[String, AnyRef], record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    if(!msg.contains("obsrv_meta")) {
      msg.put("obsrv_meta", Map(
        "syncts" -> record.timestamp(),
        "flags" -> Map(),
        "timespans" -> Map(),
        "error" -> Map()
      ))
    }
  }

  override def getProducedType: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
}

class MapSerializationSchema(topic: String, key: Option[String] = None) extends KafkaSerializationSchema[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -4284080856874185929L

  override def serialize(element: mutable.Map[String, AnyRef], timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val out = JSONUtil.serialize(element)
    key.map { kafkaKey =>
      new ProducerRecord[Array[Byte], Array[Byte]](topic, kafkaKey.getBytes(StandardCharsets.UTF_8), out.getBytes(StandardCharsets.UTF_8))
    }.getOrElse(new ProducerRecord[Array[Byte], Array[Byte]](topic, out.getBytes(StandardCharsets.UTF_8)))
  }
}
*/

class MapDeserializationSchema extends KafkaRecordDeserializationSchema[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -3224825136576915426L
  override def getProducedType: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[mutable.Map[String, AnyRef]]): Unit = {
    val msg = JSONUtil.deserialize[mutable.Map[String, AnyRef]](record.value())
    initObsrvMeta(msg, record)
    out.collect(msg)
  }

  private def initObsrvMeta(msg: mutable.Map[String, AnyRef], record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    if (!msg.contains("obsrv_meta")) {
      msg.put("obsrv_meta", Map(
        "syncts" -> record.timestamp(),
        "flags" -> Map(),
        "timespans" -> Map(),
        "error" -> Map()
      ))
    }
  }
}

class MapSerializationSchema(topic: String, key: Option[String] = None) extends KafkaRecordSerializationSchema[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -4284080856874185929L

  override def serialize(element: mutable.Map[String, AnyRef], context: KafkaRecordSerializationSchema.KafkaSinkContext, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val out = JSONUtil.serialize(element)
    key.map { kafkaKey =>
      new ProducerRecord[Array[Byte], Array[Byte]](topic, kafkaKey.getBytes(StandardCharsets.UTF_8), out.getBytes(StandardCharsets.UTF_8))
    }.getOrElse(new ProducerRecord[Array[Byte], Array[Byte]](topic, out.getBytes(StandardCharsets.UTF_8)))
  }
}
