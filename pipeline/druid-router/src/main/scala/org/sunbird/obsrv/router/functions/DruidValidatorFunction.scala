package org.sunbird.obsrv.router.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.router.domain.Event
import org.sunbird.obsrv.router.task.DruidValidatorConfig
import org.sunbird.obsrv.router.util.SchemaValidator
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.streaming.{BaseProcessFunction, Metrics}

class DruidValidatorFunction(config: DruidValidatorConfig,
                             @transient var schemaValidator: SchemaValidator = null,
                             @transient var dedupEngine: DedupEngine = null)
                            (implicit val eventTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DruidValidatorFunction])

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    if (dedupEngine == null) {
      val redisConnect = new RedisConnect(config.redisHost, config.redisPort, config)
      dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)
    }
    if (schemaValidator == null) {
      schemaValidator = new SchemaValidator(config)
    }
  }

  override def close(): Unit = {
    super.close()
    dedupEngine.closeConnectionPool()
  }

  override def metricsList(): List[String] = {
    List(config.validationSuccessMetricsCount, config.validationFailureMetricsCount,
      config.telemetryRouterMetricCount, config.summaryRouterMetricCount) ::: deduplicationMetrics
  }

  override def processElement(event: Event,
                              ctx: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {

    val isUnique =
      if (config.druidDeduplicationEnabled) {
        deDuplicate[Event, Event](event.mid(), event, ctx, config.duplicateEventOutputTag,
          flagName = "dv_duplicate")(dedupEngine, metrics)
      } else true

    if (isUnique) {
      val routeEventsDownstream =
        if (config.druidValidationEnabled) {
          validateEvent(event, ctx, metrics)
        } else true

      if (routeEventsDownstream) routeEvents(event, ctx, metrics)
    }

  }

  def validateEvent(event: Event, ctx: ProcessFunction[Event, Event]#Context, metrics: Metrics): Boolean = {

    val validationReport = schemaValidator.validate(event)

    if (validationReport.isSuccess) {
      event.markValidationSuccess()
      metrics.incCounter(config.validationSuccessMetricsCount)
    } else {
      val failedErrorMsg = schemaValidator.getInvalidFieldName(validationReport.toString)
      event.markValidationFailure(failedErrorMsg)
      metrics.incCounter(config.validationFailureMetricsCount)
      ctx.output(config.invalidEventOutputTag, event)
    }

    validationReport.isSuccess
  }

  def routeEvents(event: Event, ctx: ProcessFunction[Event, Event]#Context, metrics: Metrics): Unit = {
    if (event.isSummaryEvent) {
      metrics.incCounter(config.summaryRouterMetricCount)
      ctx.output(config.summaryRouterOutputTag, event)
    } else {
      metrics.incCounter(config.telemetryRouterMetricCount)
      ctx.output(config.telemetryRouterOutputTag, event)
    }
  }
}
