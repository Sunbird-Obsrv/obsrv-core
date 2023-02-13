package org.sunbird.obsrv.core.streaming

import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.sunbird.obsrv.core.model.ErrorConstants.Error
import org.sunbird.obsrv.core.model.SystemConfig
import org.sunbird.obsrv.core.util.Util

import java.lang
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

case class MetricsList(datasets: List[String], metrics: List[String])

case class Metrics(metrics: Map[String, ConcurrentHashMap[String, AtomicLong]]) {

  private def getMetric(dataset: String, metric: String): AtomicLong = {
    val datasetMetrics: ConcurrentHashMap[String, AtomicLong] = metrics.getOrElse(dataset, new ConcurrentHashMap[String, AtomicLong]())
    datasetMetrics.get(metric)
  }

  def incCounter(dataset: String, metric: String): Unit = {
    getMetric(dataset, metric).getAndIncrement()
  }

  def incCounter(dataset: String, metric: String, count: Long): Unit = {
    getMetric(dataset, metric).getAndAdd(count)
  }

  def getAndReset(dataset: String, metric: String): Long = {
    getMetric(dataset, metric).getAndSet(0L)
  }

  def get(dataset: String, metric: String): Long = {
    getMetric(dataset, metric).get()
  }

  def reset(dataset: String, metric: String): Unit = getMetric(dataset, metric).set(0L)
}

trait JobMetrics {
  def registerMetrics(datasets: List[String], metrics: List[String]): Metrics = {

    val allDatasets = datasets ++ List(SystemConfig.defaultDatasetId)
    val datasetMetricMap: Map[String, ConcurrentHashMap[String, AtomicLong]] = allDatasets.map(dataset => {
      val metricMap = new ConcurrentHashMap[String, AtomicLong]()
      metrics.foreach { metric => metricMap.put(metric, new AtomicLong(0L)) }
      (dataset, metricMap)
    }).toMap

    Metrics(datasetMetricMap)
  }
}

trait BaseFunction {
  private def addFlags(obsrvMeta: mutable.Map[String, AnyRef], flags: Map[String, AnyRef]) = {
    obsrvMeta.put("flags", obsrvMeta("flags").asInstanceOf[Map[String, AnyRef]] ++ flags)
  }

  private def addError(obsrvMeta: mutable.Map[String, AnyRef], error: Map[String, AnyRef]) = {
    obsrvMeta.put("error", error)
  }

  private def addTimespan(obsrvMeta: mutable.Map[String, AnyRef], jobName: String): Unit = {
    val prevTS = if (obsrvMeta.contains("prevProcessingTime")) {
      obsrvMeta("prevProcessingTime").asInstanceOf[Long]
    } else {
      obsrvMeta("syncts").asInstanceOf[Long]
    }
    val span = System.currentTimeMillis() - prevTS
    obsrvMeta.put("timespans", obsrvMeta("flags").asInstanceOf[Map[String, AnyRef]] ++ Map(jobName -> span))
  }

  def markFailed(event: mutable.Map[String, AnyRef], error: Error, jobName: String): mutable.Map[String, AnyRef] = {
    val obsrvMeta = Util.getMutableMap(event("obsrv_meta").asInstanceOf[Map[String, AnyRef]])
    addError(obsrvMeta, Map("src" -> jobName, "error_code" -> error.errorCode, "error_msg" -> error.errorMsg))
    addFlags(obsrvMeta, Map(jobName -> "failed"))
    addTimespan(obsrvMeta, jobName)
    event.put("obsrv_meta", obsrvMeta)
    event
  }

  def markSkipped(event: mutable.Map[String, AnyRef], jobName: String): mutable.Map[String, AnyRef] = {
    val obsrvMeta = Util.getMutableMap(event("obsrv_meta").asInstanceOf[Map[String, AnyRef]])
    addFlags(obsrvMeta, Map(jobName -> "skipped"))
    addTimespan(obsrvMeta, jobName)
    event.put("obsrv_meta", obsrvMeta)
    event
  }

  def markSuccess(event: mutable.Map[String, AnyRef], jobName: String): mutable.Map[String, AnyRef] = {
    val obsrvMeta = Util.getMutableMap(event("obsrv_meta").asInstanceOf[Map[String, AnyRef]])
    addFlags(obsrvMeta, Map(jobName -> "success"))
    addTimespan(obsrvMeta, jobName)
    event.put("obsrv_meta", obsrvMeta)
    event
  }

  def markComplete(event: mutable.Map[String, AnyRef]) : mutable.Map[String, AnyRef] = {
    val obsrvMeta = Util.getMutableMap(event("obsrv_meta").asInstanceOf[Map[String, AnyRef]])
    val syncts = obsrvMeta("syncts").asInstanceOf[Long]
    val span = System.currentTimeMillis() - syncts
    obsrvMeta.put("timespans", obsrvMeta("flags").asInstanceOf[Map[String, AnyRef]] ++ Map("total_processing_time" -> span))
    event.put("obsrv_meta", obsrvMeta)
    event
  }

  def containsEvent(msg: mutable.Map[String, AnyRef]): Boolean = {
    val event = msg.get("event")
    event.map(f => f.isInstanceOf[Map[String, AnyRef]]).orElse(Option(false)).get
  }
}

abstract class BaseProcessFunction[T, R](config: BaseJobConfig) extends ProcessFunction[T, R] with BaseDeduplication with JobMetrics with BaseFunction {

  private val metricsList = getMetricsList()
  private val metrics: Metrics = registerMetrics(metricsList.datasets, metricsList.metrics)

  override def open(parameters: Configuration): Unit = {
    metricsList.datasets.map { dataset =>
      metricsList.metrics.map(metric => {
        getRuntimeContext.getMetricGroup.addGroup(config.jobName).addGroup(dataset)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(dataset, metric)))
      })
    }
  }

  def processElement(event: T, context: ProcessFunction[T, R]#Context, metrics: Metrics): Unit

  def getMetricsList(): MetricsList

  override def processElement(event: T, context: ProcessFunction[T, R]#Context, out: Collector[R]): Unit = {
    processElement(event, context, metrics)
  }

}

abstract class WindowBaseProcessFunction[I, O, K](config: BaseJobConfig) extends ProcessWindowFunction[I, O, K, TimeWindow] with BaseDeduplication with JobMetrics with BaseFunction {

  private val metricsList = getMetricsList()
  private val metrics: Metrics = registerMetrics(metricsList.datasets, metricsList.metrics)

  override def open(parameters: Configuration): Unit = {
    metricsList.datasets.map { dataset =>
      metricsList.metrics.map(metric => {
        getRuntimeContext.getMetricGroup.addGroup(config.jobName).addGroup(dataset)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(dataset, metric)))
      })
    }
  }

  def getMetricsList(): MetricsList

  def process(key: K,
              context: ProcessWindowFunction[I, O, K, TimeWindow]#Context,
              elements: lang.Iterable[I],
              metrics: Metrics): Unit

  override def process(key: K, context: ProcessWindowFunction[I, O, K, TimeWindow]#Context, elements: lang.Iterable[I], out: Collector[O]): Unit = {
    process(key, context, elements, metrics)
  }

}