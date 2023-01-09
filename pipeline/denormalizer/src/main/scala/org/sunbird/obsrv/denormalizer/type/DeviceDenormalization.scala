package org.sunbird.obsrv.denormalizer.`type`

import org.sunbird.obsrv.core.streaming.Metrics
import org.sunbird.obsrv.denormalizer.domain.{DeviceProfile, Event}
import org.sunbird.obsrv.denormalizer.task.DenormalizationConfig
import org.sunbird.obsrv.denormalizer.util.CacheResponseData

class DeviceDenormalization(config: DenormalizationConfig) {

  def denormalize(event: Event, cacheData: CacheResponseData, metrics: Metrics) = {
    event.compareAndAlterEts() // Reset ets to today's date if we get future value
    val did = event.did()
    if (null != did && did.nonEmpty) {
      metrics.incCounter(config.deviceTotal)
      val deviceDetails = cacheData.device
      if (deviceDetails.nonEmpty) {
        metrics.incCounter(config.deviceCacheHit)
        event.addDeviceProfile(DeviceProfile.apply(deviceDetails))
      } else {
        metrics.incCounter(config.deviceCacheMiss)
        event.setFlag("device_denorm", value = false)
      }
    }
  }

}
