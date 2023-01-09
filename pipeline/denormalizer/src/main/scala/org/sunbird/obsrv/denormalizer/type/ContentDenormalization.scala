package org.sunbird.obsrv.denormalizer.`type`

import org.sunbird.obsrv.core.domain.EventsPath
import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.denormalizer.domain.Event
import org.sunbird.obsrv.denormalizer.task.DenormalizationConfig
import org.sunbird.obsrv.denormalizer.util.CacheResponseData

class ContentDenormalization(config: DenormalizationConfig) {

  def denormalize(event: Event, cacheData: CacheResponseData, metrics: Metrics) = {
    val objectType = event.objectType()
    val objectId = event.objectID()
    if (event.isValidEventForContentDenorm(config, objectId, objectType, event.eid())) {
      metrics.incCounter(config.contentTotal)
      val contentData = cacheData.content.map(f => {
        (f._1.toLowerCase().replace("_", ""), f._2)
      })

      if (contentData.nonEmpty) {
        metrics.incCounter(config.contentCacheHit)
        event.addContentData(contentData)
      } else {
        metrics.incCounter(config.contentCacheMiss)
        event.setFlag("content_denorm", value = false)
      }

      if (event.checkObjectIdNotEqualsRollUpId(EventsPath.OBJECT_ROLLUP_L1)) {
        event.addCollectionData(cacheData.collection.map(f => {
          (f._1.toLowerCase().replace("_", ""), f._2)
        }))
      }
      if (event.checkObjectIdNotEqualsRollUpId(EventsPath.OBJECT_ROLLUP_L2)) {
        event.addL2Data(cacheData.l2data.filter(x => config.l2DataFields.contains(x._1)).map(f => {
          (f._1.toLowerCase().replace("_", ""), f._2)
        }))
      }
    }
  }

}
