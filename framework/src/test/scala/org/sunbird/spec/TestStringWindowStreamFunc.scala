package org.sunbird.spec

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.sunbird.obsrv.core.streaming.{Metrics, MetricsList, WindowBaseProcessFunction}

import java.lang
import scala.collection.JavaConverters._

class TestStringWindowStreamFunc(config: BaseProcessTestConfig)(implicit val stringTypeInfo: TypeInformation[String])
  extends WindowBaseProcessFunction[String, String, String](config) {

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.stringEventCount)
    MetricsList(List("ALL"), metrics)
  }
  override def process(event: String,
                              context: ProcessWindowFunction[String, String, String, TimeWindow]#Context,
                              elements: lang.Iterable[String],
                              metrics: Metrics): Unit = {
    val elementsList = elements.asScala.toList
    elementsList.foreach(f => {
      context.output(config.stringOutputTag, f)
    })

    metrics.incCounter("ALL", config.stringEventCount)

    metrics.incCounter("ALL", config.mapEventCount, elementsList.size.toLong)
  }
}
