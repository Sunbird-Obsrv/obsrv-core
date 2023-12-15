package org.sunbird.obsrv.core.streaming

import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache.DedupEngine
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model._
import org.sunbird.obsrv.core.util.JSONUtil

trait BaseDeduplication {

  private[this] val logger = LoggerFactory.getLogger(classOf[BaseDeduplication])

  def isDuplicate(datasetId: String, dedupKey: Option[String], event: String)
                 (implicit deDupEngine: DedupEngine): Boolean = {

    val key = datasetId + ":" + getDedupKey(dedupKey, event)
    if (!deDupEngine.isUniqueEvent(key)) {
      true
    } else {
      deDupEngine.storeChecksum(key)
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
    if (!node.isTextual && !node.isNumber) {
      logger.warn(s"Dedup | Dedup key is not a string or number | dedupKey=$dedupKey | keyType=${node.getNodeType}")
      throw new ObsrvException(ErrorConstants.DEDUP_KEY_NOT_A_STRING_OR_NUMBER)
    }
    node.asText()
  }

}