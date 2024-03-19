package org.sunbird.obsrv.dataproducts.util

import com.typesafe.config.Config
import org.apache.logging.log4j.{LogManager, Logger}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.dataproducts.MasterDataProcessorIndexer
import org.sunbird.obsrv.model.DatasetModels.DataSource

object StorageUtil {
  val logger: Logger = LogManager.getLogger(MasterDataProcessorIndexer.getClass)
  val dayPeriodFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC()

  case class Paths(datasourceRef: String, ingestionPath: String, outputFilePath: String, timestamp: Long)

  case class BlobProvider(sparkURIFormat: String, ingestionSourceType: String, druidURIFormat: String)

  // This method returns a BlobProvider object based on cloud storage provider
  def providerFormat(cloudProvider: String): BlobProvider = {
    cloudProvider match {
      case "local" => BlobProvider("file", "local", "file")
      case "aws" => BlobProvider("s3a", "s3", "s3")
      case "azure" => BlobProvider("wasbs", "azure", "azure")
      case "gcloud" => BlobProvider("gs", "google", "gs")
      case "cephs3" => BlobProvider("s3a", "s3", "s3") // TODO: Have to check Druid compatibility
      case "oci" => BlobProvider("s3a", "s3", "s3") // TODO: Have to check Druid compatibility
      case _ => throw new ObsrvException(ErrorConstants.UNSUPPORTED_PROVIDER)
    }
  }

  def getPaths(datasource: DataSource, config: Config): Paths = {
    val dt = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay()
    val timestamp = dt.getMillis
    val date = dayPeriodFormat.print(dt)
    val provider = providerFormat(config.getString("cloud.storage.provider"))
    val cloudPrefix = provider.sparkURIFormat + config.getString("cloud.storage.container")
    val pathSuffix = s"""masterdata-indexer/${datasource.datasetId}/$date/"""
    val ingestionPath = cloudPrefix.replace(provider.sparkURIFormat, provider.druidURIFormat) + pathSuffix
    val datasourceRef = datasource.datasource + '-' + date
    val outputFilePath = cloudPrefix + pathSuffix
    Paths(datasourceRef, ingestionPath, outputFilePath, timestamp)
  }

  // This method provides appropriate input source spec depending on the cloud storage provider
  def getInputSourceSpec(filePath: String, config: Config): String = {
    config.getString("source.spec").replace("FILE_PATH", filePath)
  }

}