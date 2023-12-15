package org.sunbird.obsrv.streaming

import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.sunbird.obsrv.core.model.FunctionalError.FunctionalError
import org.sunbird.obsrv.core.model.Models._
import org.sunbird.obsrv.core.model.Producer.Producer
import org.sunbird.obsrv.core.model.Stats.Stats
import org.sunbird.obsrv.core.model.StatusCode.StatusCode
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.streaming._
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.Dataset
import org.sunbird.obsrv.registry.DatasetRegistry

import java.lang
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.mutable

trait SystemEventHandler {
  private def getStatus(flags: Map[String, AnyRef], producer: Producer): Option[StatusCode] = {
    flags.get(producer.toString).map(f => StatusCode.withName(f.asInstanceOf[String]))
  }

  private def getTime(timespans: Map[String, AnyRef], producer: Producer): Option[Long] = {
    timespans.get(producer.toString).map(f => f.asInstanceOf[Long])
  }

  private def getStat(obsrvMeta: Map[String, AnyRef], stat: Stats): Option[Long] = {
    obsrvMeta.get(stat.toString).map(f => f.asInstanceOf[Long])
  }

  def getError(error: ErrorConstants.Error, producer: Producer, functionalError: FunctionalError): Option[ErrorLog] = {
    Some(ErrorLog(pdata_id = producer, pdata_status = StatusCode.failed, error_type = functionalError, error_code = error.errorCode, error_message = error.errorMsg, error_level = ErrorLevel.critical, error_count = Some(1)))
  }

  def generateSystemEvent(dataset: Option[String], event: mutable.Map[String, AnyRef], config: BaseJobConfig[_], producer: Producer, error: Option[ErrorLog] = None, dataset_type: Option[String] = None): String = {
    val obsrvMeta = event("obsrv_meta").asInstanceOf[Map[String, AnyRef]]
    val flags = obsrvMeta("flags").asInstanceOf[Map[String, AnyRef]]
    val timespans = obsrvMeta("timespans").asInstanceOf[Map[String, AnyRef]]

    JSONUtil.serialize(SystemEvent(
      EventID.METRIC, ctx = ContextData(module = ModuleID.processing, pdata = PData(config.jobName, PDataType.flink, Some(producer)), dataset = dataset, dataset_type = dataset_type),
      data = EData(error = error, pipeline_stats = Some(PipelineStats(extractor_events = None,
        extractor_status = getStatus(flags, Producer.extractor), extractor_time = getTime(timespans, Producer.extractor),
        validator_status = getStatus(flags, Producer.validator), validator_time = getTime(timespans, Producer.validator),
        dedup_status = getStatus(flags, Producer.dedup), dedup_time = getTime(timespans, Producer.dedup),
        denorm_status = getStatus(flags, Producer.denorm), denorm_time = getTime(timespans, Producer.denorm),
        transform_status = getStatus(flags, Producer.transformer), transform_time = getTime(timespans, Producer.transformer),
        total_processing_time = getStat(obsrvMeta, Stats.total_processing_time), latency_time = getStat(obsrvMeta, Stats.latency_time), processing_time = getStat(obsrvMeta, Stats.processing_time)
      )))
    ))
  }

  def getDatasetId(dataset: Option[String], config: BaseJobConfig[_]): String = {
    dataset.getOrElse(config.defaultDatasetID)
  }

}

abstract class BaseDatasetProcessFunction(config: BaseJobConfig[mutable.Map[String, AnyRef]])
  extends BaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]](config) with SystemEventHandler {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  def getMetrics(): List[String]

  override def getMetricsList(): MetricsList = {
    val metrics = getMetrics() ++ List(config.eventFailedMetricsCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  private def initMetrics(datasetId: String): Unit = {
    if(!metrics.hasDataset(datasetId)) {
      val metricMap = new ConcurrentHashMap[String, AtomicLong]()
      metricsList.metrics.map(metric => {
        metricMap.put(metric, new AtomicLong(0L))
        getRuntimeContext.getMetricGroup.addGroup(config.jobName).addGroup(datasetId)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(datasetId, metric)))
      })
      metrics.initDataset(datasetId, metricMap)
    }
  }

  def markFailure(datasetId: Option[String], event: mutable.Map[String, AnyRef], ctx: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                 metrics: Metrics, error: ErrorConstants.Error, producer: Producer, functionalError: FunctionalError, datasetType: Option[String] = None): Unit = {

    metrics.incCounter(getDatasetId(datasetId, config), config.eventFailedMetricsCount)
    ctx.output(config.failedEventsOutputTag(), super.markFailed(event, error, producer))
    val errorLog = getError(error, producer, functionalError)
    val systemEvent = generateSystemEvent(Some(getDatasetId(datasetId, config)), event, config, producer, errorLog, datasetType)
    ctx.output(config.systemEventsOutputTag, systemEvent)
  }

  def markCompletion(dataset: Dataset, event: mutable.Map[String, AnyRef], ctx: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context, producer: Producer): Unit = {
    ctx.output(config.systemEventsOutputTag, generateSystemEvent(Some(dataset.id), super.markComplete(event, dataset.dataVersion), config, producer, dataset_type = Some(dataset.datasetType)))
  }

  def processElement(dataset: Dataset, event: mutable.Map[String, AnyRef],context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context, metrics: Metrics): Unit
  override def processElement(event: mutable.Map[String, AnyRef], context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context, metrics: Metrics): Unit = {

    val datasetIdOpt = event.get(config.CONST_DATASET)
    if (datasetIdOpt.isEmpty) {
      markFailure(None, event, context, metrics, ErrorConstants.MISSING_DATASET_ID, Producer.validator, FunctionalError.MissingDatasetId)
      return
    }
    val datasetId = datasetIdOpt.get.asInstanceOf[String]
    initMetrics(datasetId)
    val datasetOpt = DatasetRegistry.getDataset(datasetId)
    if (datasetOpt.isEmpty) {
      markFailure(Some(datasetId), event, context, metrics, ErrorConstants.MISSING_DATASET_CONFIGURATION, Producer.validator, FunctionalError.MissingDatasetId)
      return
    }
    val dataset = datasetOpt.get
    if (!super.containsEvent(event)) {
      markFailure(Some(datasetId), event, context, metrics, ErrorConstants.EVENT_MISSING, Producer.validator, FunctionalError.MissingEventData)
      return
    }
    processElement(dataset, event, context, metrics)
  }
}

abstract class BaseDatasetWindowProcessFunction(config: BaseJobConfig[mutable.Map[String, AnyRef]])
  extends WindowBaseProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String](config) with SystemEventHandler {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  def getMetrics(): List[String]

  override def getMetricsList(): MetricsList = {
    val metrics = getMetrics() ++ List(config.eventFailedMetricsCount)
    MetricsList(DatasetRegistry.getDataSetIds(config.datasetType()), metrics)
  }

  private def initMetrics(datasetId: String): Unit = {
    if(!metrics.hasDataset(datasetId)) {
      val metricMap = new ConcurrentHashMap[String, AtomicLong]()
      metricsList.metrics.map(metric => {
        metricMap.put(metric, new AtomicLong(0L))
        getRuntimeContext.getMetricGroup.addGroup(config.jobName).addGroup(datasetId)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(datasetId, metric)))
      })
      metrics.initDataset(datasetId, metricMap)
    }
  }

  def markFailure(datasetId: Option[String], event: mutable.Map[String, AnyRef], ctx: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context,
                  metrics: Metrics, error: ErrorConstants.Error, producer: Producer, functionalError: FunctionalError, datasetType: Option[String] = None): Unit = {
    metrics.incCounter(getDatasetId(datasetId, config), config.eventFailedMetricsCount)
    ctx.output(config.failedEventsOutputTag(), super.markFailed(event, error, producer))
    val errorLog = getError(error, producer, functionalError)
    val systemEvent = generateSystemEvent(Some(getDatasetId(datasetId, config)), event, config, producer, errorLog, datasetType)
    ctx.output(config.systemEventsOutputTag, systemEvent)
  }

  def markCompletion(dataset: Dataset, event: mutable.Map[String, AnyRef], ctx: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, producer: Producer): Unit = {
    ctx.output(config.systemEventsOutputTag, generateSystemEvent(Some(dataset.id), super.markComplete(event, dataset.dataVersion), config, producer, dataset_type = Some(dataset.datasetType)))
  }

  def processWindow(dataset: Dataset, context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, elements: List[mutable.Map[String, AnyRef]], metrics: Metrics): Unit
  override def process(datasetId: String, context: ProcessWindowFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef], String, TimeWindow]#Context, elements: lang.Iterable[mutable.Map[String, AnyRef]], metrics: Metrics): Unit = {

    initMetrics(datasetId)
    val datasetOpt = DatasetRegistry.getDataset(datasetId)
    val eventsList = elements.asScala.toList
    if (datasetOpt.isEmpty) {
      eventsList.foreach(event => {
        markFailure(Some(datasetId), event, context, metrics, ErrorConstants.MISSING_DATASET_CONFIGURATION, Producer.validator, FunctionalError.MissingDatasetId)
      })
      return
    }
    val dataset = datasetOpt.get
    val buffer = mutable.Buffer[mutable.Map[String, AnyRef]]()
    eventsList.foreach(event => {
      if (!super.containsEvent(event)) {
        markFailure(Some(datasetId), event, context, metrics, ErrorConstants.EVENT_MISSING, Producer.validator, FunctionalError.MissingEventData)
      } else {
        buffer.append(event)
      }
    })

    if(buffer.nonEmpty) {
      processWindow(dataset, context, buffer.toList, metrics)
    }
  }
}