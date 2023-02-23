package org.sunbird.spec

import scala.collection.mutable.Map
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}


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
    context.output(config.mapOutputTag, event)
  }
}
