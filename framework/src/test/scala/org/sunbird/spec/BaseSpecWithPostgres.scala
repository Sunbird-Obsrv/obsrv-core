package org.sunbird.spec

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import redis.embedded.RedisServer

class BaseSpecWithPostgres extends FlatSpec with BeforeAndAfterAll {

  var embeddedPostgres: EmbeddedPostgres = _
  var redisServer: RedisServer = _

  override def beforeAll() {
    super.beforeAll()
    redisServer = new RedisServer(6340)
    redisServer.start()
    embeddedPostgres = EmbeddedPostgres.builder.setPort(5432).start() // Defaults to 5432 port
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    redisServer.stop()
    embeddedPostgres.close()
  }

}
