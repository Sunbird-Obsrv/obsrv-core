package org.sunbird.obsrv.dataproducts

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory}
import kong.unirest.Unirest
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.native.JsonMethods._
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.model.DatasetModels.{DataSource, Dataset}
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.collection.mutable

object MasterDataProcessorIndexer {

  private val config: Config = ConfigFactory.load("application.conf").withFallback(ConfigFactory.systemEnvironment())
  private val dayPeriodFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC()

  private case class Paths(datasourceRef: String, objectKey: String, outputFilePath: String, timestamp: Long)

  def main(args: Array[String]): Unit = {

    val datasets = DatasetRegistry.getAllDatasets("master-dataset")
    val indexedDatasets = datasets.filter(dataset => dataset.datasetConfig.indexData.nonEmpty && dataset.datasetConfig.indexData.get)
    indexedDatasets.foreach(dataset => {
      indexDataset(dataset)
    })
  }

  private def indexDataset(dataset: Dataset): Unit = {
    val datasources = DatasetRegistry.getDatasources(dataset.id)
    if(datasources.isEmpty || datasources.get.size > 1) {
      return
    }
    val datasource = datasources.get.head
    val paths = getPaths(datasource)
    createDataFile(dataset, paths.timestamp, paths.outputFilePath, paths.objectKey)
    val ingestionSpec = updateIngestionSpec(datasource, paths.datasourceRef, paths.objectKey)
    submitIngestionTask(ingestionSpec)
    updateDataSourceRef(datasource, paths.datasourceRef)
    if(!datasource.datasource.equals(datasource.datasourceRef)) {
      deleteDataSource(datasource.datasourceRef)
    }
  }

  private def getPaths(datasource: DataSource): Paths = {

    val dt = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay()
    val timestamp = dt.getMillis
    val date = dayPeriodFormat.print(dt)
    val objectKey = "masterdata-indexer/" + datasource.datasetId + "/" + date + ".json"
    val datasourceRef = datasource.datasource + '-' + date
    val outputFilePath = "masterdata-indexer/" + datasource.datasetId + "/" + date
    Paths(datasourceRef, objectKey, outputFilePath, timestamp)
  }
  private def updateIngestionSpec(datasource: DataSource, datasourceRef: String, objectKey: String): String = {

    val deltaIngestionSpec = s"""{"type":"index_parallel","spec":{"dataSchema":{"dataSource":"$datasourceRef"},"ioConfig":{"type":"index_parallel"},"tuningConfig":{"type":"index_parallel","targetPartitionSize":5000000,"maxRowsInMemory":25000,"forceExtendableShardSpecs":false,"logParseExceptions":true}}}"""
    val provider = getProvider()
    val container = config.getString("cloudStorage.container")
    val inputSourceSpec = s"""{"spec":{"ioConfig":{"inputSource":{"type":"$provider","objectGlob":"**.json","objects":[{"bucket":"$container","path":"$objectKey"}]}}}}"""

    val deltaJson = parse(deltaIngestionSpec)
    val inputSourceJson = parse(inputSourceSpec)
    val ingestionSpec = parse(datasource.ingestionSpec)

    val modIngestionSpec = ingestionSpec merge deltaJson merge inputSourceJson
    compact(render(modIngestionSpec))
  }

  @throws[Exception]
  private def getProvider(): String = {
    config.getString("cloudStorage.provider") match {
      case "aws" => "s3"
      case "azure" => "azure"
      case "gcloud" => "google"
      case "cephs3" => "s3" // TODO: Have to check Druid compatibility
      case "oci" => "s3" // TODO: Have to check Druid compatibility
      case _ => throw new Exception("Unsupported provider")
    }
  }

  private def submitIngestionTask(ingestionSpec: String) = {
    // TODO: Handle success and failure responses properly
    val response = Unirest.post(config.getString("druid.indexer.url"))
      .header("Content-Type", "application/json")
      .body(ingestionSpec).asJson()
    response.ifFailure(response => throw new Exception("Exception while submitting ingestion task"))
  }

  private def updateDataSourceRef(datasource: DataSource, datasourceRef: String): Unit = {
    DatasetRegistry.updateDatasourceRef(datasource, datasourceRef)
  }

  private def deleteDataSource(datasourceRef: String): Unit = {
    // TODO: Handle success and failure responses properly
    val response = Unirest.delete(config.getString("druid.datasource.delete.url") + datasourceRef)
      .header("Content-Type", "application/json")
      .asJson()
    response.ifFailure(response => throw new Exception("Exception while deleting datasource" + datasourceRef))
  }

  private def createDataFile(dataset: Dataset, timestamp: Long, outputFilePath: String, objectKey: String): String = {

    val conf = new SparkConf()
      .setAppName("MasterDataProcessorIndexer")
      .setMaster("local[4]")
      .set("spark.redis.host", dataset.datasetConfig.redisDBHost.get)
      .set("spark.redis.port", String.valueOf(dataset.datasetConfig.redisDBHost.get))
      .set("spark.redis.db", String.valueOf(dataset.datasetConfig.redisDB.get))

    val sc = new SparkContext(conf)

    val readWriteConf = ReadWriteConfig(scanCount = 1000, maxPipelineSize = 1000)
    val rdd = sc.fromRedisKV("*")(readWriteConfig = readWriteConf)
      .map(f => JSONUtil.deserialize[mutable.Map[String, AnyRef]](f._2))
      .map(f => f.put("syncts", timestamp.asInstanceOf[AnyRef]))
      .map(f => JSONUtil.serialize(f))
      .coalesce(1)
      .saveAsTextFile(outputFilePath)
    sc.stop()

    val storageService = StorageServiceFactory.getStorageService(StorageConfig(config.getString("cloudStorage.provider"), config.getString("cloudStorage.accountName"), config.getString("cloudStorage.accountKey")))
    storageService.upload(config.getString("cloudStorage.container"), outputFilePath + "/part-00000", objectKey, isDirectory = Option(false))
  }

}
