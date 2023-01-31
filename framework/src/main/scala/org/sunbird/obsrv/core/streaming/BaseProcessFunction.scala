package org.sunbird.obsrv.core.streaming

import java.lang
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector
import org.sunbird.obsrv.core.model.SystemConfig
import org.sunbird.obsrv.core.util.Util

import scala.collection.mutable

case class MetricsList(val datasets: List[String], val metrics: List[String]);

case class Metrics(metrics: Map[String, ConcurrentHashMap[String, AtomicLong]]) {

  private def getMetric(dataset: String, metric: String): AtomicLong = {
    val datasetMetrics: ConcurrentHashMap[String, AtomicLong] = metrics.getOrElse(dataset, SystemConfig.defaultDatasetId).asInstanceOf[ConcurrentHashMap[String, AtomicLong]];
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

    val datasetMetricMap:Map[String, ConcurrentHashMap[String, AtomicLong]] = datasets.map(dataset => {
      val metricMap = new ConcurrentHashMap[String, AtomicLong]()
      metrics.map { metric => metricMap.put(metric, new AtomicLong(0L)) }
      (dataset, metricMap)
    }).toMap

    Metrics(datasetMetricMap)
  }
}

abstract class BaseProcessFunction[T, R](config: BaseJobConfig) extends ProcessFunction[T, R] with BaseDeduplication with JobMetrics {

  private val metricsList = getMetricsList();
  private val metrics: Metrics = registerMetrics(metricsList.datasets, metricsList.metrics)

  override def open(parameters: Configuration): Unit = {
    metricsList.datasets.map { dataset =>
      metricsList.metrics.map(metric => {
        getRuntimeContext.getMetricGroup.addGroup(config.jobName).addGroup(dataset)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(dataset, metric)));
      })
    }
  }

  def processElement(event: T, context: ProcessFunction[T, R]#Context, metrics: Metrics): Unit
  def getMetricsList(): MetricsList

  override def processElement(event: T, context: ProcessFunction[T, R]#Context, out: Collector[R]): Unit = {
    processElement(event, context, metrics)
  }

  def getMutableMap(immutableMap: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    Util.getMutableMap(immutableMap)
  }

  def addFlags(event: mutable.Map[String, AnyRef], flags: Map[String, AnyRef]) = {
    if (event.get("obsrv_sys_flags").isDefined) {
      event.put("obsrv_sys_flags", (event.get("obsrv_sys_flags").getOrElse(Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]] ++ flags))
    } else {
      event.put("obsrv_sys_flags", flags)
    }
  }

  def addError(event: mutable.Map[String, AnyRef], error: Map[String, AnyRef]) = {
    if (event.get("obsrv_process_error").isDefined) {
      event.put("obsrv_process_error", (event.get("obsrv_process_error").getOrElse(Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]] ++ error))
    } else {
      event.put("obsrv_process_error", error)
    }
  }
}

abstract class WindowBaseProcessFunction[I, O, K](config: BaseJobConfig) extends ProcessWindowFunction[I, O, K, TimeWindow] with BaseDeduplication with JobMetrics {

  private val metricsList = getMetricsList();
  private val metrics: Metrics = registerMetrics(metricsList.datasets, metricsList.metrics)

  override def open(parameters: Configuration): Unit = {
    metricsList.datasets.map { dataset =>
      metricsList.metrics.map(metric => {
        getRuntimeContext.getMetricGroup.addGroup(config.jobName).addGroup(dataset)
          .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(dataset, metric)));
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

  def getMutableMap(immutableMap: Map[String, AnyRef]): mutable.Map[String, AnyRef] = {
    val mutableMap = mutable.Map[String, AnyRef]();
    mutableMap ++= immutableMap;
    mutableMap
  }

  def addFlags(event: mutable.Map[String, AnyRef], flags: Map[String, AnyRef]) = {
    if (event.get("obsrv_sys_flags").isDefined) {
      event.put("obsrv_sys_flags", (event.get("obsrv_sys_flags").getOrElse(Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]] ++ flags))
    } else {
      event.put("obsrv_sys_flags", flags)
    }
  }

  def addError(event: mutable.Map[String, AnyRef], error: Map[String, AnyRef]) = {
    if (event.get("obsrv_process_error").isDefined) {
      event.put("obsrv_process_error", (event.get("obsrv_process_error").getOrElse(Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]] ++ error))
    } else {
      event.put("obsrv_process_error", error)
    }
  }
}