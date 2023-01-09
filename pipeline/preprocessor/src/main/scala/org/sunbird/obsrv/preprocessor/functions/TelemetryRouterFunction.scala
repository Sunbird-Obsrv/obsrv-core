package org.sunbird.obsrv.preprocessor.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.preprocessor.domain.Event
import org.sunbird.obsrv.preprocessor.task.PipelinePreprocessorConfig
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics}

class TelemetryRouterFunction(config: PipelinePreprocessorConfig)
                             (implicit val eventTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[TelemetryRouterFunction])

  override def metricsList(): List[String] = {
    List(config.primaryRouterMetricCount,
      config.logEventsRouterMetricsCount,
      config.errorEventsRouterMetricsCount,
      config.auditEventRouterMetricCount,
      config.shareEventsRouterMetricCount
    )
  }

  override def processElement(event: Event,
                              ctx: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {


    event.eid().toUpperCase() match {
      case "AUDIT" =>
        ctx.output(config.auditRouteEventsOutputTag, event)
        metrics.incCounter(metric = config.auditEventRouterMetricCount)
        metrics.incCounter(metric = config.primaryRouterMetricCount) // Since we are are sinking the AUDIT Event into primary router topic
      case "SHARE" =>
        ctx.output(config.shareRouteEventsOutputTag, event)
        metrics.incCounter(metric = config.shareEventsRouterMetricCount)
        metrics.incCounter(metric = config.primaryRouterMetricCount) // // Since we are are sinking the SHARE Event into primary router topic
      case "LOG" =>
        ctx.output(config.logEventsOutputTag, event)
        metrics.incCounter(metric = config.logEventsRouterMetricsCount)
      case "ERROR" =>
        ctx.output(config.errorEventOutputTag, event)
        metrics.incCounter(metric = config.errorEventsRouterMetricsCount)
      case _ => ctx.output(config.primaryRouteEventsOutputTag, event)
        metrics.incCounter(metric = config.primaryRouterMetricCount)
    }
  }
}
