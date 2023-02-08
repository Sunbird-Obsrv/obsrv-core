package org.sunbird.obsrv.core.model

object SystemConfig {

  // TODO: Fetch the system config from postgres db
  val defaultDedupPeriodInSeconds: Int = 604800 // 7 days
  val maxEventSize: Long = 1048576
  val defaultDatasetId = "ALL"

}
