package org.sunbird.spec

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
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

}
