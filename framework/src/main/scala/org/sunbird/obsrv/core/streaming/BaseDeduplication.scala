package org.sunbird.obsrv.core.streaming

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache.DedupEngine
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.core.model.Models.{PData, SystemEvent}
import org.sunbird.obsrv.core.util.JSONUtil

import scala.collection.mutable

trait BaseDeduplication {

  private[this] val logger = LoggerFactory.getLogger(classOf[BaseDeduplication])

  def isDuplicate(dedupKey: Option[String], event: String,
                  context: ProcessFunction[mutable.Map[String, AnyRef], mutable.Map[String, AnyRef]]#Context,
                  config: BaseJobConfig[_])
                 (implicit deDupEngine: DedupEngine): Boolean = {

    try {
      val key = getDedupKey(dedupKey, event)
      if (!deDupEngine.isUniqueEvent(key)) {
        logger.debug(s"Event with mid: $key is duplicate")
        true
      } else {
        deDupEngine.storeChecksum(key)
        false
      }
    } catch {
      case ex: ObsrvException =>
        val sysEvent = SystemEvent(PData(config.jobName, "flink", "deduplication"), Map("error_code" -> ex.error.errorCode, "error_msg" -> ex.error.errorMsg))
        context.output(config.systemEventsOutputTag, JSONUtil.serialize(sysEvent))
        false
    }
  }

  private def getDedupKey(dedupKey: Option[String], event: String): String = {
    if (dedupKey.isEmpty) {
      throw new ObsrvException(ErrorConstants.NO_DEDUP_KEY_FOUND)
    }
    val node = JSONUtil.getKey(dedupKey.get, event)
    if (node.isMissingNode) {
      throw new ObsrvException(ErrorConstants.NO_DEDUP_KEY_FOUND)
    }
    if (!node.isTextual) {
      throw new ObsrvException(ErrorConstants.DEDUP_KEY_NOT_A_STRING)
    }
    node.asText()
  }

}
