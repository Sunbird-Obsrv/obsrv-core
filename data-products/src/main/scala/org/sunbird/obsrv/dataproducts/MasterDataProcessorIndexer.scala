package org.sunbird.obsrv.dataproducts

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.native.JsonMethods._
import org.sunbird.obsrv.core.exception.ObsrvException
import org.sunbird.obsrv.core.model.ErrorConstants
import org.sunbird.obsrv.dataproducts.helper.BaseMetricHelper
import org.sunbird.obsrv.dataproducts.model.{Edata, MetricLabel}
import org.sunbird.obsrv.dataproducts.util.{CommonUtil, HttpUtil, StorageUtil}
import org.sunbird.obsrv.model.DatasetModels.{DataSource, Dataset}
import org.sunbird.obsrv.model.DatasetStatus
import org.sunbird.obsrv.registry.DatasetRegistry

object MasterDataProcessorIndexer {
  private final val logger: Logger = LogManager.getLogger(MasterDataProcessorIndexer.getClass)

  @throws[ObsrvException]
  def processDataset(config: Config, dataset: Dataset, spark: SparkSession): Map[String, Long] = {
    val result = CommonUtil.time {
      val datasource = fetchDatasource(dataset)
      val paths = StorageUtil.getPaths(datasource, config)
      val eventsCount: Long = createDataFile(dataset, paths.outputFilePath, spark, config)
      val ingestionSpec: String = updateIngestionSpec(datasource, paths.datasourceRef, paths.ingestionPath, config)
      if (eventsCount > 0L) {
        submitIngestionTask(dataset.id, ingestionSpec, config)
      }
      DatasetRegistry.updateDatasourceRef(datasource, paths.datasourceRef)
      if (!datasource.datasourceRef.equals(paths.datasourceRef)) {
        deleteDataSource(dataset.id, datasource.datasourceRef, config)
      }
      Map("success_dataset_count" -> 1, "total_dataset_count" -> 1, "total_events_processed" -> eventsCount)
    }
    val metricMap = result._2 ++ Map("total_time_taken" -> result._1)
    metricMap.asInstanceOf[Map[String, Long]]
  }

  // This method is used to update the ingestion spec based on datasource and storage path
  private def updateIngestionSpec(datasource: DataSource, datasourceRef: String, filePath: String, config: Config): String = {
    val deltaIngestionSpec: String = config.getString("delta.ingestion.spec").replace("DATASOURCE_REF", datasourceRef)
    val inputSourceSpec: String = StorageUtil.getInputSourceSpec(filePath, config)
    val deltaJson = parse(deltaIngestionSpec)
    val inputSourceJson = parse(inputSourceSpec)
    val ingestionSpec = parse(datasource.ingestionSpec)
    val modIngestionSpec = ingestionSpec merge deltaJson merge inputSourceJson
    compact(render(modIngestionSpec))
  }

  // This method is used to submit the ingestion task to Druid for indexing data
  def submitIngestionTask(datasetId: String, ingestionSpec: String, config: Config): Unit = {
    logger.debug(s"submitIngestionTask() | datasetId=$datasetId")
    val response = HttpUtil.post(config.getString("druid.indexer.url"), ingestionSpec)
    response.ifFailure(throw new ObsrvException(ErrorConstants.ERR_SUBMIT_INGESTION_FAILED))
  }

  // This method is used for deleting a datasource from druid
  private def deleteDataSource(datasetID: String, datasourceRef: String, config: Config): Unit = {
    logger.debug(s"deleteDataSource() | datasetId=$datasetID")
    val response = HttpUtil.delete(config.getString("druid.datasource.delete.url") + datasourceRef)
    response.ifFailure(throw new ObsrvException(ErrorConstants.ERR_DELETE_DATASOURCE_FAILED))
  }

  // This method will fetch the data from redis based on dataset config
  // then write the data as a compressed JSON to the respective cloud provider
  private def createDataFile(dataset: Dataset, outputFilePath: String, spark: SparkSession, config: Config): Long = {
    logger.info(s"createDataFile() | START | dataset=${dataset.id} ")
    import spark.implicits._
    val readWriteConf = ReadWriteConfig(scanCount = config.getInt("redis.scan.count"), maxPipelineSize = config.getInt("redis.max.pipeline.size"))
    val redisConfig = new RedisConfig(initialHost = RedisEndpoint(host = dataset.datasetConfig.redisDBHost.get, port = dataset.datasetConfig.redisDBPort.get, dbNum = dataset.datasetConfig.redisDB.get))
    val ts: Long = new DateTime(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis
    val rdd = spark.sparkContext.fromRedisKV("*")(redisConfig = redisConfig, readWriteConfig = readWriteConf).map(
      f => CommonUtil.processEvent(f._2, ts)
    )
    val noOfRecords: Long = rdd.count()
    if (noOfRecords > 0) {
      rdd.toDF().write.mode("overwrite").option("compression", "gzip").json(outputFilePath)
    }
    logger.info(s"createDataFile() | END | dataset=${dataset.id} | noOfRecords=$noOfRecords")
    noOfRecords
  }

  private def getDatasets(): List[Dataset] = {
    val datasets: List[Dataset] = DatasetRegistry.getAllDatasets("master-dataset")
    datasets.filter(dataset => {
      dataset.datasetConfig.indexData.nonEmpty && dataset.datasetConfig.indexData.get && dataset.status == DatasetStatus.Live
    })
  }

  def fetchDatasource(dataset: Dataset): DataSource = {
    val datasources: List[DataSource] = DatasetRegistry.getDatasources(dataset.id).get
    if (datasources.isEmpty) {
      throw new ObsrvException(ErrorConstants.ERR_DATASOURCE_NOT_FOUND)
    }
    datasources.head
  }

  // This method will fetch the dataset from database and processes the dataset
  // then generates required metrics
  def processDatasets(config: Config, spark: SparkSession): Unit = {
    val datasets: List[Dataset] = getDatasets()
    val metricHelper = new BaseMetricHelper(config)
    datasets.foreach(dataset => {
      logger.info(s"processDataset() | START | datasetId=${dataset.id}")
      val metricData = try {
        val metrics = processDataset(config, dataset, spark)
        logger.info(s"processDataset() | SUCCESS | datasetId=${dataset.id} | Metrics=$metrics")
        Edata(metric = metrics, labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloud.storage.provider")}")))
      } catch {
        case ex: ObsrvException =>
          logger.error(s"processDataset() | FAILED | datasetId=${dataset.id} | Error=${ex.error}", ex)
          Edata(metric = Map(metricHelper.getMetricName("failure_dataset_count") -> 1, "total_dataset_count" -> 1), labels = List(MetricLabel("job", "MasterDataIndexer"), MetricLabel("datasetId", dataset.id), MetricLabel("cloud", s"${config.getString("cloud.storage.provider")}")), err = ex.error.errorCode, errMsg = ex.error.errorMsg)
      }
      metricHelper.generate(datasetId = dataset.id, edata = metricData)
    })
  }

  // $COVERAGE-OFF$
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("masterdata-indexer.conf").withFallback(ConfigFactory.systemEnvironment())
    val spark = CommonUtil.getSparkSession("MasterDataIndexer", config)
    processDatasets(config, spark)
    spark.stop()
  }
  // $COVERAGE-ON$
}