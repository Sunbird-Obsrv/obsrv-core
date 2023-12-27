package org.sunbird.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.obsrv.core.cache.{DedupEngine, RedisConnect}
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.streaming.BaseDeduplication
class BaseDeduplicationTestSpec extends BaseSpec with Matchers with MockitoSugar {

  val config: Config = ConfigFactory.load("base-test.conf")
  val baseConfig = new BaseProcessTestConfig(config)
  val SAMPLE_EVENT: String = """{"dataset":"d1","event":{"id":"1234","vehicleCode":"HYUN-CRE-D6","date":"2023-03-01","dealer":{"dealer":"KUNUnited","locationId":"KUN1"},"metrics":{"bookingsTaken":50,"deliveriesPromised":20,"deliveriesDone":19}}}""".stripMargin

  "BaseDeduplicationTestSpec" should "be able to cover all scenarios of deduplication check" in {
    val redisConnection = new RedisConnect(baseConfig.redisHost, baseConfig.redisPort, baseConfig.redisConnectionTimeout)
    val dedupEngine = new DedupEngine(redisConnection, 0, 4309535)
    val dedupFn = new DeduplicationFn(dedupEngine)

    dedupFn.validateDedup("d1", Some("event.id"), SAMPLE_EVENT) should be (false)
    dedupFn.validateDedup("d1", Some("event.id"), SAMPLE_EVENT) should be (true)

    the[ObsrvException] thrownBy {
      dedupFn.validateDedup("d1", Some("event"), SAMPLE_EVENT)
    } should have message ErrorConstants.DEDUP_KEY_NOT_A_STRING_OR_NUMBER.errorMsg

    the[ObsrvException] thrownBy {
      dedupFn.validateDedup("d1", Some("event.mid"), SAMPLE_EVENT)
    } should have message ErrorConstants.NO_DEDUP_KEY_FOUND.errorMsg

    the[ObsrvException] thrownBy {
      dedupFn.validateDedup("d1", None, SAMPLE_EVENT)
    } should have message ErrorConstants.NO_DEDUP_KEY_FOUND.errorMsg

  }
}

class DeduplicationFn(dedupEngine: DedupEngine) extends BaseDeduplication {

  def validateDedup(datasetId: String, dedupKey: Option[String], event: String):Boolean = {
    isDuplicate(datasetId, dedupKey, event)(dedupEngine)
  }

}