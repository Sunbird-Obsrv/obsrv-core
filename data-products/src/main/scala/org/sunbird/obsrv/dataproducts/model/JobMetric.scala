package org.sunbird.obsrv.dataproducts.model

import java.util.UUID

case class Actor(id: String, `type`: String)

case class Context(env: String, pdata: Pdata)

case class Edata(metric: Map[String, Any], labels: Seq[MetricLabel], err: String = null, errMsg: String = null)

case class MetricLabel(key: String, value: String)

case class MetricObject(id: String, `type`: String, ver: String)

case class Pdata(id: String, pid: String, ver: String)

trait IJobMetric {
  val eid: String
  val ets: Long
  val mid: String
  val actor: Actor
  val context: Context
  val `object`: MetricObject
  val edata: Edata
}

case class JobMetric(eid: String = "METRIC", ets: Long, mid: String = UUID.randomUUID().toString, actor: Actor, context: Context, `object`: MetricObject, edata: Edata) extends IJobMetric

