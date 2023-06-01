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
  }

  "DedupEngine functionality" should "be able to identify if the key is unique or duplicate & it should able throw jedis excption for invalid action" in  intercept[JedisException] {
    val redisConnection = new RedisConnect(baseConfig.redisHost, baseConfig.redisPort, baseConfig.redisConnectionTimeout)
    val dedupEngine = new DedupEngine(redisConnection, 2, 200)
    dedupEngine.getRedisConnection should not be (null)
    dedupEngine.isUniqueEvent("key-1") should be(true)
    dedupEngine.storeChecksum("key-1")
    dedupEngine.isUniqueEvent("key-1") should be(false)
    dedupEngine.isUniqueEvent(null)
    dedupEngine.closeConnectionPool()
  }

  it should "be able to reconnect when a jedis exception for invalid action is thrown" in intercept[JedisException] {
    val redisConnection = new RedisConnect(baseConfig.redisHost, baseConfig.redisPort, baseConfig.redisConnectionTimeout)
    val dedupEngine = new DedupEngine(redisConnection, 0, 4309535)
    dedupEngine.isUniqueEvent("event-id-3") should be(true)
    dedupEngine.storeChecksum(null)
    dedupEngine.getRedisConnection should not be(null)
  }



  "RestUtil functionality" should "be able to return response" in {
    val restUtil = new RestUtil()
    val url = "https://httpbin.org/json";
    val response = restUtil.get(url);
    response should not be null
  }

}