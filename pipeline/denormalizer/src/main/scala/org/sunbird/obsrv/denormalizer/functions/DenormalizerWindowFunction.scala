package org.sunbird.obsrv.denormalizer.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.streaming.{Metrics, MetricsList, WindowBaseProcessFunction}
import org.sunbird.obsrv.denormalizer.task.DenormalizerConfig
import org.sunbird.obsrv.denormalizer.util._
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry

import java.lang
import scala.collection.JavaConverters._
import scala.collection.mutable

class DenormalizerWindowFunction(config: DenormalizerConfig)(implicit val eventTypeInfo: TypeInformation[mutable.Map[String, AnyRef]])
  extends WindowBaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormalizerWindowFunction])

  private[this] var denormCache: DenormCache = _

  override def getMetricsList(): MetricsList = {
    val metrics = List(config.denormSuccess, config.denormTotal, config.denormFailed, config.eventsSkipped)
    MetricsList(DatasetRegistry.getDataSetIds(), metrics)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    denormCache = new DenormCache(config)
    denormCache.open(DatasetRegistry.getAllDatasets())
  }

  override def close(): Unit = {
    super.close()
    denormCache.close()
  }

  override def process(key: String, context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, elements: lang.Iterable[mutable.Map[String, AnyRef]], metrics: Metrics): Unit = {

    val eventsList = elements.asScala.toList
    val msg = eventsList.head
    val datasetId = msg("dataset").asInstanceOf[String] // DatasetId cannot be empty at this stage
    metrics.incCounter(datasetId, config.denormTotal, eventsList.size.toLong)
    val dataset = DatasetRegistry.getDataset(datasetId).get
    val denormEvents = eventsList.map(msg => {
      DenormEvent(msg, None, None)
    })

    if (dataset.denormConfig.isDefined) {
      denormalize(denormEvents, dataset, metrics, context)
    } else {
      metrics.incCounter(datasetId, config.eventsSkipped, eventsList.size.toLong)
      eventsList.foreach(msg => {
        context.output(config.denormEventsTag, markSkipped(msg))
      })
    }
  }

  private def denormalize(events: List[DenormEvent], dataset: Dataset, metrics: Metrics,
                          context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context): Unit = {

    val datasetId = dataset.id

    val denormEvents = denormCache.denormMultipleEvents(datasetId, events, dataset.denormConfig.get.denormFields)
    denormEvents.foreach(denormEvent => {
      if (denormEvent.error.isEmpty) {
        metrics.incCounter(datasetId, config.denormSuccess)
        context.output(config.denormEventsTag, markSuccess(denormEvent.msg))
      } else {
        metrics.incCounter(datasetId, config.denormFailed)
        context.output(config.denormFailedTag, markFailed(denormEvent.msg, denormEvent.error.get))
      }
    })
  }

  private def markSkipped(event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("denorm_processed" -> "skipped"))
    event
  }

  private def markFailed(event: mutable.Map[String, AnyRef], error: Error): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("denorm_processed" -> "no"))
    addError(event, Map("src" -> config.jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    event
  }

  private def markSuccess(event: mutable.Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    addFlags(event, Map("denorm_processed" -> "yes"))
    event
  }

}