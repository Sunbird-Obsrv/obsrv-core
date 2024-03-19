package org.sunbird.obsrv.dataproducts.util

import com.typesafe.config.Config
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.{DataSource, Dataset}

import scala.collection.mutable

object CommonUtil {

  private final val logger: Logger = LogManager.getLogger(CommonUtil.getClass)

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

  def processEvent(value: String, ts: Long) = {
    val json = JSONUtil.deserialize[mutable.Map[String, AnyRef]](value)
    json("obsrv_meta") = mutable.Map[String, AnyRef]("syncts" -> ts.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
    JSONUtil.serialize(json)
  }

  private def getSafeConfigString(config: Config, key: String): String = {
    if (config.hasPath(key)) config.getString(key) else ""
  }

  def getSparkSession(appName: String, config: Config): SparkSession = {

    val conf = new SparkConf().setAppName(appName)
    val master = getSafeConfigString(config, "spark.master")

    if (master.isEmpty) {
      logger.info("Master not found. Setting it to local[*]")
      conf.setMaster("local[*]")
    }
    SparkSession.builder().appName(appName).config(conf).getOrCreate()
  }

}