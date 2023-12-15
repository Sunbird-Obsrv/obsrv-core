package org.sunbird.obsrv.core.cache

import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisException


class DedupEngine(redisConnect: RedisConnect, store: Int, expirySeconds: Int) extends Serializable {

  private val serialVersionUID = 6089562751616425354L
  private[this] var redisConnection: Jedis = redisConnect.getConnection
  redisConnection.select(store)

  @throws[JedisException]
  def isUniqueEvent(checksum: String): Boolean = {
    !redisConnection.exists(checksum)
  }

  @throws[JedisException]
  def storeChecksum(checksum: String): Unit = {
    redisConnection.setex(checksum, expirySeconds, "")
  }

  def getRedisConnection: Jedis = redisConnection

  def closeConnectionPool(): Unit = {
    redisConnection.close()
  }
}
