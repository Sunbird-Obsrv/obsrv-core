package org.sunbird.spec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics, MetricsList}

class TestStringStreamFunc(config: BaseProcessTestConfig)(implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[String, String](config) {

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.stringEventCount)
    MetricsList(List("ALL"), metrics)
  }
  override def processElement(event: String,
                              context: ProcessFunction[String, String]#Context,
                              metrics: Metrics): Unit = {
    context.output(config.stringOutputTag, event)
    metrics.incCounter("ALL", config.stringEventCount)
  }
}
