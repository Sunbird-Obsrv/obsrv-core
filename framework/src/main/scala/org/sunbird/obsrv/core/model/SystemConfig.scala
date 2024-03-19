package org.sunbird.obsrv.core.model

import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.Models.SystemSetting
import org.sunbird.obsrv.core.util.{PostgresConnect, PostgresConnectionConfig}

import java.io.File
import java.sql.ResultSet

object SystemConfig {

  private def getSystemConfig(key: String): Option[SystemSetting] = {
    SystemConfigService.getSystemSetting(key)
  }

  @throws[ObsrvException]
  private def getConfigValueOpt(key: String, requiredType: String): Option[String] = {

    getSystemConfig(key).map(config => {
      if (!config.valueType.equalsIgnoreCase(requiredType)) throw new ObsrvException(ErrorConstants.SYSTEM_SETTING_INVALID_TYPE)
      config.value
    }).orElse(None)
  }

  private def getConfigValue(key: String, requiredType: String): String = {

    getSystemConfig(key).map(config => {
      if (!config.valueType.equalsIgnoreCase(requiredType)) throw new ObsrvException(ErrorConstants.SYSTEM_SETTING_INVALID_TYPE)
      config.value
    }).orElse(throw new ObsrvException(ErrorConstants.SYSTEM_SETTING_NOT_FOUND)).get
  }

  def getString(key: String): String = {
    getConfigValue(key, requiredType = "string")
  }

  def getString(key: String, defaultValue: String): String = {
    getConfigValueOpt(key, requiredType = "string").getOrElse(defaultValue)
  }

  def getInt(key: String): Int = {
    getConfigValue(key, requiredType = "int").toInt
  }

  def getInt(key: String, defaultValue: Int): Int = {
    getConfigValueOpt(key, requiredType = "int").getOrElse(defaultValue.toString).toInt
  }

  def getLong(key: String): Long = {
    getConfigValue(key, requiredType = "long").toLong
  }

  def getLong(key: String, defaultValue: Long): Long = {
    getConfigValueOpt(key, requiredType = "long").getOrElse(defaultValue.toString).toLong
  }

  def getBoolean(key: String): Boolean = {
    getConfigValue(key, requiredType = "boolean").toBoolean
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getConfigValueOpt(key, requiredType = "boolean").getOrElse(defaultValue.toString).toBoolean
  }

}

object SystemConfigService {

  private val configFile = new File("/data/flink/conf/baseconfig.conf")
  // $COVERAGE-OFF$
  val config: Config = if (configFile.exists()) {
    println("Loading configuration file cluster baseconfig.conf...")
    ConfigFactory.parseFile(configFile).resolve()
  } else {
    // $COVERAGE-ON$
    println("Loading configuration file baseconfig.conf inside the jar...")
    ConfigFactory.load("baseconfig.conf").withFallback(ConfigFactory.systemEnvironment())
  }
  private val postgresConfig = PostgresConnectionConfig(
    config.getString("postgres.user"),
    config.getString("postgres.password"),
    config.getString("postgres.database"),
    config.getString("postgres.host"),
    config.getInt("postgres.port"),
    config.getInt("postgres.maxConnections"))

  @throws[Exception]
  def getAllSystemSettings: List[SystemSetting] = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    try {
      val rs = postgresConnect.executeQuery("SELECT * FROM system_settings")
      val result = Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        parseSystemSetting(result)
      }).toList
      result
    } finally {
      postgresConnect.closeConnection()
    }
  }

  @throws[Exception]
  def getSystemSetting(key: String): Option[SystemSetting] = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    try {
      val rs = postgresConnect.executeQuery(s"SELECT * FROM system_settings WHERE key = '$key'")
      if (rs.next) Option(parseSystemSetting(rs)) else None
    } finally {
      postgresConnect.closeConnection()
    }
  }

  private def parseSystemSetting(rs: ResultSet): SystemSetting = {
    val key = rs.getString("key")
    val value = rs.getString("value")
    val category = rs.getString("category")
    val valueType = rs.getString("valuetype")
    val label = rs.getString("label")

    SystemSetting(key, value, category, valueType, Option(label))
  }
}