package org.sunbird.spec

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.sunbird.obsrv.core.util.PostgresConnect
import redis.embedded.RedisServer

class BaseSpecWithPostgres extends FlatSpec with BeforeAndAfterAll {

  var embeddedPostgres: EmbeddedPostgres = _
  var redisServer: RedisServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6340)
    try {
      redisServer.start()
    } catch {
      case _: Exception => Console.err.println("### Unable to start redis server. Falling back to use locally run redis if any ###")
    }
    embeddedPostgres = EmbeddedPostgres.builder.setPort(5432).start() // Defaults to 5432 port
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    redisServer.stop()
    embeddedPostgres.close()
  }

  def createSystemSettings(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS system_settings ( key text NOT NULL, value text NOT NULL, category text NOT NULL DEFAULT 'SYSTEM'::text, valuetype text NOT NULL, created_date timestamp NOT NULL DEFAULT now(), updated_date timestamp, label text, PRIMARY KEY (\"key\"));")
    postgresConnect.execute("insert into system_settings values('defaultDedupPeriodInSeconds', '604801', 'system', 'int', now(),  now(), 'Dedup Period in Seconds');")
    postgresConnect.execute("insert into system_settings values('maxEventSize', '1048676', 'system', 'long', now(),  now(), 'Max Event Size');")
    postgresConnect.execute("insert into system_settings values('defaultDatasetId', 'ALL', 'system', 'string', now(),  now(), 'Default Dataset Id');")
    postgresConnect.execute("insert into system_settings values('encryptionSecretKey', 'ckW5GFkTtMDNGEr5k67YpQMEBJNX3x2f', 'system', 'string', now(),  now(), 'Encryption Secret Key');")
    postgresConnect.execute("insert into system_settings values('enable', 'true', 'system', 'boolean', now(),  now(), 'Enable flag');")
  }

  def clearSystemSettings(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("DROP TABLE system_settings;")
  }
}
