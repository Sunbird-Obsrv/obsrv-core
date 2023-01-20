package org.sunbird.obsrv.core.streaming

import com.google.gson.Gson
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.cache.DedupEngine
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.{ErrorConstants, SystemConfig}

trait BaseDeduplication {

  private[this] val logger = LoggerFactory.getLogger(classOf[BaseDeduplication])
  val uniqueEventMetricCount = "unique-event-count"
  val duplicateEventMetricCount = "duplicate-event-count"

  def deDup[T, R](key: String,
                  event: T,
                  context: ProcessFunction[T, R]#Context,
                  successOutputTag: OutputTag[R],
                  duplicateOutputTag: OutputTag[R],
                  flagName: String
                 )(implicit deDupEngine: DedupEngine, metrics: Metrics): Unit = {

    if (null != key && !deDupEngine.isUniqueEvent(key)) {
      logger.debug(s"Event with mid: $key is duplicate")
      metrics.incCounter(duplicateEventMetricCount)
      context.output(duplicateOutputTag, event.asInstanceOf[R])
    } else {
      if (key != null) {
        logger.debug(s"Adding mid: $key to Redis")
        deDupEngine.storeChecksum(key)
      }
      metrics.incCounter(uniqueEventMetricCount)
      logger.debug(s"Pushing the event with mid: $key for further processing")
      context.output(successOutputTag, event.asInstanceOf[R])
    }
  }

  def isDuplicate(dedupKey: Option[String], event: String)(implicit deDupEngine: DedupEngine): Boolean = {

    try {
      val key = getDedupKey(dedupKey, event);
      if (!deDupEngine.isUniqueEvent(key)) {
        logger.debug(s"Event with mid: $key is duplicate")
        true
      } else {
        deDupEngine.storeChecksum(key)
        false
      }
    } catch {
      case ex: ObsrvException =>
        // TODO: Create System event of failed deduplication. Skip dedup check
        false
    }
  }

  private def getDedupKey(dedupKey: Option[String], event: String): String = {
    if (!dedupKey.isDefined) {
      throw new ObsrvException(ErrorConstants.NO_DEDUP_KEY_FOUND);
    }
    val node = JSONUtil.getKey(dedupKey.get, event);
    if (node.isMissingNode) {
      throw new ObsrvException(ErrorConstants.NO_DEDUP_KEY_FOUND);
    }
    if(!node.isTextual) {
      throw new ObsrvException(ErrorConstants.DEDUP_KEY_NOT_A_STRING);
    }
    node.asText()
  }

  def deduplicationMetrics: List[String] = {
    List(uniqueEventMetricCount, duplicateEventMetricCount)
  }
}
