package org.sunbird.spec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.model.{Constants, ErrorConstants, Producer}
import org.sunbird.obsrv.core.streaming.{BaseDeduplication, BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.JSONUtil

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.collection.mutable.Map


class TestMapStreamFunc(config: BaseProcessTestMapConfig)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Map[String, AnyRef], Map[String, AnyRef]](config) with BaseDeduplication {

  override def getMetricsList(): MetricsList = {
    MetricsList(List("ALL"), List(config.mapEventCount))
  }

  override def processElement(event: Map[String, AnyRef],
                              context: ProcessFunction[Map[String, AnyRef], Map[String, AnyRef]]#Context,
                              metrics: Metrics): Unit = {
    metrics.get("ALL", config.mapEventCount)
    metrics.reset("ALL", config.mapEventCount)
    metrics.incCounter("ALL", config.mapEventCount)
    metrics.getAndReset("ALL", config.mapEventCount)
    assert(metrics.hasDataset("ALL"))
    metrics.initDataset("d2", new ConcurrentHashMap[String, AtomicLong]())

    context.output(config.mapOutputTag, mutable.Map(Constants.TOPIC -> config.kafkaMapOutputTopic, Constants.MESSAGE -> event))

    super.markSuccess(event, Producer.extractor)
    super.markFailed(event, ErrorConstants.NO_IMPLEMENTATION_FOUND, Producer.extractor)
    super.markSkipped(event, Producer.extractor)
    super.markComplete(event, None)
    super.markPartial(event, Producer.extractor)
    assert(super.containsEvent(event))
    assert(!super.containsEvent(mutable.Map("test" -> "123".asInstanceOf[AnyRef])))
    assert(!super.containsEvent(Map("dataset" -> "d1")))

    val eventStr = JSONUtil.serialize(event)
    val code = JSONUtil.getKey("event.vehicleCode", eventStr).textValue()
    val redisConnection = new RedisConnect(config.redisHost, config.redisPort, config.redisConnectionTimeout)
    implicit val dedupEngine = new DedupEngine(redisConnection, 2, 200)
    val isDup = super.isDuplicate("D1", Option("event.id"), eventStr)
    code match {
      case "HYUN-CRE-D6" => assert(!isDup)
      case "HYUN-CRE-D7" => assert(isDup)
    }

    assert(!super.isDuplicate("D1", None, eventStr))
    assert(!super.isDuplicate("D1", Option("mid"), eventStr))
    assert(!super.isDuplicate("D1", Option("event"), eventStr))
  }
}
