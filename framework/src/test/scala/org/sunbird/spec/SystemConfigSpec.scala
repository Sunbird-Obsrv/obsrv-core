package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.obsrv.core.model.{SystemConfig, SystemConfigService}
import org.sunbird.obsrv.core.util.{PostgresConnect, PostgresConnectionConfig}

class SystemConfigSpec extends BaseSpecWithPostgres with Matchers with MockFactory {
  val configFile: Config = ConfigFactory.load("base-test.conf")
  val postgresConfig: PostgresConnectionConfig = PostgresConnectionConfig(
    configFile.getString("postgres.user"),
    configFile.getString("postgres.password"),
    configFile.getString("postgres.database"),
    configFile.getString("postgres.host"),
    configFile.getInt("postgres.port"),
    configFile.getInt("postgres.maxConnections"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val postgresConnect = new PostgresConnect(postgresConfig)
    createSystemSettings(postgresConnect)
  }

  override def afterAll(): Unit = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    clearSystemSettings(postgresConnect)
    super.afterAll()
  }

  def createInvalidSystemSettings(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS system_settings ( key text NOT NULL, value text NOT NULL, category text NOT NULL DEFAULT 'SYSTEM'::text, valuetype text NOT NULL, created_date timestamp NOT NULL DEFAULT now(), updated_date timestamp, label text, PRIMARY KEY (\"key\"));")
    postgresConnect.execute("insert into system_settings values('defaultDedupPeriodInSeconds', '604801', 'system', 'double', now(),  now(), 'Dedup Period in Seconds');")
    postgresConnect.execute("insert into system_settings values('maxEventSize', '1048676', 'system', 'inv', now(),  now(), 'Max Event Size');")
    postgresConnect.execute("insert into system_settings values('defaultDatasetId', 'ALL', 'system', 'random', now(),  now(), 'Default Dataset Id');")
    postgresConnect.execute("insert into system_settings values('encryptionSecretKey', 'ckW5GFkTtMDNGEr5k67YpQMEBJNX3x2f', 'system', 'text', now(),  now(), 'Encryption Secret Key');")
  }

  "SystemConfig" should "populate configurations with values from database" in {
    SystemConfig.getInt("defaultDedupPeriodInSeconds") should be(604801)
    SystemConfig.getInt("defaultDedupPeriodInSeconds", 604800) should be(604801)
    SystemConfig.getLong("maxEventSize", 100L) should be(1048676L)
    SystemConfig.getString("defaultDatasetId", "NEW") should be("ALL")
    SystemConfig.getString("encryptionSecretKey", "test") should be("ckW5GFkTtMDNGEr5k67YpQMEBJNX3x2f")
    SystemConfig.getBoolean("enable", false) should be(true)
  }

  "SystemConfig" should "return default values when keys are not present in db" in {
    val postgresConnect = new PostgresConnect(postgresConfig)
    postgresConnect.execute("TRUNCATE TABLE system_settings;")
    SystemConfig.getInt("defaultDedupPeriodInSeconds", 604800) should be(604800)
    SystemConfig.getLong("maxEventSize", 100L) should be(100L)
    SystemConfig.getString("defaultDatasetId", "NEW") should be("NEW")
    SystemConfig.getString("encryptionSecretKey", "test") should be("test")
    SystemConfig.getBoolean("enable", false) should be(false)
  }

  "SystemConfig" should "throw exception when valueType doesn't match" in {
    val postgresConnect = new PostgresConnect(postgresConfig)
    clearSystemSettings(postgresConnect)
    createInvalidSystemSettings(postgresConnect)
    val thrown = intercept[Exception] {
      SystemConfig.getInt("defaultDedupPeriodInSeconds", 604800)
    }
    thrown.getMessage should be("Invalid value type for system setting")
  }

  "SystemConfig" should "throw exception when valueType doesn't match without default value" in {
    val postgresConnect = new PostgresConnect(postgresConfig)
    clearSystemSettings(postgresConnect)
    createInvalidSystemSettings(postgresConnect)
    val thrown = intercept[Exception] {
      SystemConfig.getInt("defaultDedupPeriodInSeconds")
    }
    thrown.getMessage should be("Invalid value type for system setting")
  }

  "SystemConfigService" should "return all system settings" in {
    val systemSettings = SystemConfigService.getAllSystemSettings
    systemSettings.size should be(4)
    systemSettings.map(f => {
      f.key match {
        case "defaultDedupPeriodInSeconds" => f.value should be("604801")
        case "maxEventSize" => f.value should be("1048676")
        case "defaultDatasetId" => f.value should be("ALL")
        case "encryptionSecretKey" => f.value should be("ckW5GFkTtMDNGEr5k67YpQMEBJNX3x2f")
        case "enable" => f.value should be("true")
      }
    })
  }

  "SystemConfig" should "throw exception when the key is not present in db" in {
    var thrown = intercept[Exception] {
      SystemConfig.getInt("invalidKey")
    }
    thrown.getMessage should be("System setting not found for requested key")

    thrown = intercept[Exception] {
      SystemConfig.getString("invalidKey")
    }
    thrown.getMessage should be("System setting not found for requested key")

    thrown = intercept[Exception] {
      SystemConfig.getBoolean("invalidKey")
    }
    thrown.getMessage should be("System setting not found for requested key")

    thrown = intercept[Exception] {
      SystemConfig.getLong("invalidKey")
    }
    thrown.getMessage should be("System setting not found for requested key")
  }

}
