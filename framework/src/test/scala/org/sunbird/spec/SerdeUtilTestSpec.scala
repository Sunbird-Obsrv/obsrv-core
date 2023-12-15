package org.sunbird.spec

import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import org.scalamock.matchers.ArgCapture.CaptureAll
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.obsrv.core.serde.{MapDeserializationSchema, StringDeserializationSchema}

import java.nio.charset.StandardCharsets
import scala.collection.mutable

class SerdeUtilTestSpec extends FlatSpec with Matchers with MockFactory {



  "SerdeUtil" should "test all the map and string deserialization classes" in {

    val strCollector: Collector[String] = mock[Collector[String]]
    val mapCollector: Collector[mutable.Map[String, AnyRef]] = mock[Collector[mutable.Map[String, AnyRef]]]
    val key = "key1".getBytes(StandardCharsets.UTF_8)
    val validEvent = """{"event":{"id":1234}}""".getBytes(StandardCharsets.UTF_8)
    val eventWithObsrvMeta = """{"event":{"id":1234},"obsrv_meta":{}}""".getBytes(StandardCharsets.UTF_8)
    val invalidEvent = """{"event":{"id":1234}""".getBytes(StandardCharsets.UTF_8)

    val validRecord = new ConsumerRecord[Array[Byte], Array[Byte]]("test-topic", 0, 1234l, 1701447470737l, TimestampType.CREATE_TIME, -1l, -1, -1, key, validEvent)
    val validRecordWithObsrvMeta = new ConsumerRecord[Array[Byte], Array[Byte]]("test-topic", 0, 1234l, 1701447470737l, TimestampType.CREATE_TIME, -1l, -1, -1, key, eventWithObsrvMeta)
    val invalidRecord = new ConsumerRecord[Array[Byte], Array[Byte]]("test-topic", 0, 1234l, 1701447470737l, TimestampType.CREATE_TIME, -1l, -1, -1, key, invalidEvent)


    val sds = new StringDeserializationSchema()
    (strCollector.collect _).expects("""{"event":{"id":1234}}""")
    sds.deserialize(validRecord, strCollector)

    val c = CaptureAll[mutable.Map[String, AnyRef]]()
    val mds = new MapDeserializationSchema()
    mapCollector.collect _ expects capture(c) repeat 3
    mds.deserialize(validRecord, mapCollector)
    mds.deserialize(validRecordWithObsrvMeta, mapCollector)
    mds.deserialize(invalidRecord, mapCollector)
    //(mapCollector.collect _).verify(*).once()
    val validMsg: mutable.Map[String, AnyRef] = c.values.apply(0)
    val validMsgWithObsrvMeta: mutable.Map[String, AnyRef] = c.values.apply(1)
    val invalidMsg: mutable.Map[String, AnyRef] = c.values.apply(2)
    Console.println("validMsg", validMsg)
    validMsg.get("obsrv_meta").isDefined should be (true)
    val validObsrvMeta = validMsg.get("obsrv_meta").get.asInstanceOf[Map[String, AnyRef]]
    val validEventMsg = validMsg.get("event").get.asInstanceOf[Map[String, AnyRef]]
    validObsrvMeta.get("syncts").get.asInstanceOf[Long] should be (1701447470737l)
    validObsrvMeta.get("processingStartTime").get.asInstanceOf[Long] should be >= 1701447470737l
    validEventMsg.get("id").get.asInstanceOf[Int] should be (1234)

    Console.println("validMsgWithObsrvMeta", validMsgWithObsrvMeta)
    validMsgWithObsrvMeta.get("obsrv_meta").isDefined should be(true)
    validMsgWithObsrvMeta.get("event").isDefined should be(true)
    val validObsrvMeta2 = validMsgWithObsrvMeta.get("obsrv_meta").get.asInstanceOf[Map[String, AnyRef]]
    val validEventMsg2 = validMsgWithObsrvMeta.get("event").get.asInstanceOf[Map[String, AnyRef]]
    validObsrvMeta2.keys.size should be(0)
    validEventMsg2.get("id").get.asInstanceOf[Int] should be (1234)

    Console.println("invalidMsg", invalidMsg)
    invalidMsg.get("obsrv_meta").isDefined should be(true)
    invalidMsg.get("event").isDefined should be(false)
    val invalidObsrvMeta = invalidMsg.get("obsrv_meta").get.asInstanceOf[Map[String, AnyRef]]
    val invalidEventMsg = invalidMsg.get("invalid_json").get.asInstanceOf[String]
    invalidObsrvMeta.get("syncts").get.asInstanceOf[Long] should be(1701447470737l)
    invalidObsrvMeta.get("processingStartTime").get.asInstanceOf[Long] should be >= 1701447470737l
    invalidEventMsg should be("""{"event":{"id":1234}""")
  }

  it should "test generic serialization schema" in {

  }
}
