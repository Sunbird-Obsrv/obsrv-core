package org.sunbird.spec

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import redis.embedded.RedisServer

class BaseSpec extends FlatSpec with BeforeAndAfterAll {

  var redisServer: RedisServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6340)
    try {
      redisServer.start()
    } catch {
      case _: Exception => Console.err.println("### Unable to start redis server. Falling back to use locally run redis if any ###")
    }

  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    redisServer.stop()
  }

}
