package org.sunbird.obsrv.denormalizer.`type`

import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.denormalizer.domain.Event
import org.sunbird.obsrv.denormalizer.task.DenormalizationConfig
import org.sunbird.obsrv.denormalizer.util.CacheResponseData

class DialcodeDenormalization(config: DenormalizationConfig) {

  def denormalize(event: Event, cacheData: CacheResponseData, metrics: Metrics) = {
    if (null != event.objectType() && List("dialcode", "qr").contains(event.objectType().toLowerCase())) {
      metrics.incCounter(config.dialcodeTotal)
      val dialcodeData = cacheData.dialCode.map(f => {(f._1.toLowerCase().replace("_", ""), f._2)})

      if (dialcodeData.nonEmpty) {
        metrics.incCounter(config.dialcodeCacheHit)
        event.addDialCodeData(dialcodeData)
      } else {
        metrics.incCounter(config.dialcodeCacheMiss)
        event.setFlag("dialcode_denorm", value = false)
      }
    }
  }

}
