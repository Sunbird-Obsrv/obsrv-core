package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.util.RestUtil
import redis.clients.jedis.exceptions.JedisException

class RedisTestSpec extends BaseSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("base-test.conf")
  val baseConfig = new BaseProcessTestConfig(config)
  val SAMPLE_EVENT: String = """{"dataset":"d1","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealer":"KUNUnited","locationId":"KUN1"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}""".stripMargin

  "RedisConnect functionality" should "be able to connect to redis" in {
    val redisConnection = new RedisConnect(baseConfig.redisHost, baseConfig.redisPort, baseConfig.redisConnectionTimeout)
    val status = redisConnection.getConnection(2)
    status.isConnected should be(true)

    val status2 = redisConnection.getConnection(2, 1000l)
    status2.isConnected should be(true)
  }

  "DedupEngine functionality" should "be able to identify if the key is unique or duplicate & it should able throw jedis exception for invalid action" in  {
    val redisConnection = new RedisConnect(baseConfig.redisHost, baseConfig.redisPort, baseConfig.redisConnectionTimeout)
    val dedupEngine = new DedupEngine(redisConnection, 2, 200)
    dedupEngine.getRedisConnection should not be null
    dedupEngine.isUniqueEvent("key-1") should be(true)
    dedupEngine.storeChecksum("key-1")
    dedupEngine.isUniqueEvent("key-1") should be(false)
    a[JedisException] should be thrownBy {dedupEngine.isUniqueEvent(null)}
    dedupEngine.closeConnectionPool()
  }

  it should "be able to reconnect when a jedis exception for invalid action is thrown" in {
    val redisConnection = new RedisConnect(baseConfig.redisHost, baseConfig.redisPort, baseConfig.redisConnectionTimeout)
    val dedupEngine = new DedupEngine(redisConnection, 0, 4309535)
    dedupEngine.isUniqueEvent("event-id-3") should be(true)
    a[JedisException] should be thrownBy {dedupEngine.storeChecksum(null)}
    dedupEngine.getRedisConnection should not be null
  }

  "RestUtil functionality" should "be able to return response" in {
    val restUtil = new RestUtil()
    val url = "https://httpbin.org/json"
    val response = restUtil.get(url, Some(Map("x-auth" -> "123")))
    response should not be null

    val response2 = restUtil.get("https://httpbin.org/json")
    response2 should not be null
  }

}