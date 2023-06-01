package org.sunbird.obsrv.core.model

object SystemConfig {

  // TODO: Fetch the system config from postgres db
  val defaultDedupPeriodInSeconds: Int = 604800 // 7 days
  val maxEventSize: Long = 1048576
  val defaultDatasetId = "ALL"

  // secret key length should be 16, 24 or 32 characters
  val encryptionSecretKey = "ckW5GFkTtMDNGEr5k67YpQMEBJNX3x2f"

}
