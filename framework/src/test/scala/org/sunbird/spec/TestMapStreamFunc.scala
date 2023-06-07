package org.sunbird.spec

import scala.collection.mutable.Map
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}
import org.sunbird.obsrv.core.util.JSONUtil


class TestMapStreamFunc(config: BaseProcessTestMapConfig)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[Map[String, AnyRef], Map[String, AnyRef]](config) {

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
    context.output(config.mapOutputTag, event)

    super.markSuccess(event, "test-job")
    super.markFailed(event, ErrorConstants.NO_IMPLEMENTATION_FOUND, config.jobName)
    super.markSkipped(event, config.jobName)
    super.markComplete(event, None)
    assert(super.containsEvent(event))
    assert(!super.containsEvent(Map("dataset" -> "d1")))

    val eventStr = JSONUtil.serialize(event)
    val code = JSONUtil.getKey("event.vehicleCode", eventStr).textValue()
    val redisConnection = new RedisConnect(config.redisHost, config.redisPort, config.redisConnectionTimeout)
    implicit val dedupEngine = new DedupEngine(redisConnection, 2, 200)
    val isDup = super.isDuplicate("D1", Option("event.id"), eventStr, context, config)
    code match {
      case "HYUN-CRE-D6" => assert(!isDup)
      case "HYUN-CRE-D7" => assert(isDup)
    }

    assert(!super.isDuplicate("D1", None, eventStr, context, config))
    assert(!super.isDuplicate("D1", Option("mid"), eventStr, context, config))
    assert(!super.isDuplicate("D1", Option("event"), eventStr, context, config))
  }
}
