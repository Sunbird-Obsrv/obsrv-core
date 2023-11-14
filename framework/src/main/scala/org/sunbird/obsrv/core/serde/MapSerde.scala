package org.sunbird.obsrv.core.serde

import java.nio.charset.StandardCharsets
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.{Constants, ErrorConstants}
import org.sunbird.obsrv.core.util.{JSONUtil, Util}

import scala.collection.mutable


class MapDeserializationSchema extends KafkaRecordDeserializationSchema[mutable.Map[String, AnyRef]] {

  private val serialVersionUID = -3224825136576915426L
  override def getProducedType: TypeInformation[mutable.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[mutable.Map[String, AnyRef]])
  private[this] val logger = LoggerFactory.getLogger(classOf[MapDeserializationSchema])

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[mutable.Map[String, AnyRef]]): Unit = {
    try {
      val msg = JSONUtil.deserialize[mutable.Map[String, AnyRef]](record.value())
      initObsrvMeta(msg, record)
      out.collect(msg)
    } catch {
      case ex: Exception =>
        logger.error("Error while deserializing the JSON event")
        ex.printStackTrace()
        val invalidEvent = mutable.Map[String, AnyRef]()
        invalidEvent.put(Constants.EVENT, new String(record.value, "UTF-8"))
        initObsrvMeta(invalidEvent, record)
        addError(invalidEvent, ErrorConstants.ERR_INVALID_EVENT.copy(errorReason = ex.getMessage))
        out.collect(invalidEvent)
    }
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

  private def addError(event:mutable.Map[String, AnyRef], error: ErrorConstants.ErrorValue): Unit ={
    val obsrvMeta = Util.getMutableMap(event(Constants.OBSRV_META).asInstanceOf[Map[String, AnyRef]])
    obsrvMeta.put(Constants.ERROR, Map(Constants.ERROR_CODE -> error.errorCode))
    event.put(Constants.OBSRV_META, obsrvMeta.toMap)
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
