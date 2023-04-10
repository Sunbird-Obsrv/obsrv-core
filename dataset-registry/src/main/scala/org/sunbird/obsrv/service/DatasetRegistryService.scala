package org.sunbird.obsrv.service

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.BaseDeduplication
import org.sunbird.obsrv.core.util.{JSONUtil, PostgresConnect, PostgresConnectionConfig}
import org.sunbird.obsrv.model.DatasetModels.{Dataset, DatasetConfig, DatasetTransformation, DedupConfig, DenormConfig, ExtractionConfig, RouterConfig, TransformationFunction, ValidationConfig}

import java.io.File
import java.sql.ResultSet

object DatasetRegistryService {

  private[this] val logger = LoggerFactory.getLogger(DatasetRegistryService.getClass)

  private val configFile = new File("/data/conf/base-config.conf")
  val config: Config = if (configFile.exists()) {
    ConfigFactory.parseFile(configFile).resolve()
  } else {
    ConfigFactory.load("base-config.conf").withFallback(ConfigFactory.systemEnvironment())
  }
  private val postgresConfig = PostgresConnectionConfig(config.getString("postgres.user"), config.getString("postgres.password"),
    config.getString("postgres.database"), config.getString("postgres.host"), config.getInt("postgres.port"),
    config.getInt("postgres.maxConnections"))

  def readAllDatasets(): Map[String, Dataset] = {

    val postgresConnect = new PostgresConnect(postgresConfig)
    try {
      val rs = postgresConnect.executeQuery("SELECT * FROM datasets")
      Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val dataset = parseDataset(result)
        (dataset.id, dataset)
      }).toMap
    } catch {
      case ex: Exception =>
        logger.error("Exception while reading datasets from Postgres", ex)
        Map()
    } finally {
      postgresConnect.closeConnection()
    }
  }

  def readAllDatasetTransformations(): Map[String, List[DatasetTransformation]] = {

    val postgresConnect = new PostgresConnect(postgresConfig)
    try {
      val rs = postgresConnect.executeQuery("SELECT * FROM dataset_transformations")
      Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val dt = parseDatasetTransformation(result)
        (dt.datasetId, dt)
      }).toList.groupBy(f => f._1).mapValues(f => f.map(x => x._2))
    } catch {
      case ex: Exception =>
        logger.error("Exception while reading dataset transformations from Postgres", ex)
        Map()
    } finally {
      postgresConnect.closeConnection()
    }
  }


  private def parseDataset(rs: ResultSet): Dataset = {
    val datasetId = rs.getString("id")
    val datasetType = rs.getString("type")
    val validationConfig = rs.getString("validation_config")
    val extractionConfig = rs.getString("extraction_config")
    val dedupConfig = rs.getString("dedup_config")
    val jsonSchema = rs.getString("data_schema")
    val denormConfig = rs.getString("denorm_config")
    val routerConfig = rs.getString("router_config")
    val datasetConfig = rs.getString("dataset_config")
    val status = rs.getString("status")

    Dataset(datasetId, datasetType,
      if (extractionConfig == null) None else Some(JSONUtil.deserialize[ExtractionConfig](extractionConfig)),
      if (dedupConfig == null) None else Some(JSONUtil.deserialize[DedupConfig](dedupConfig)),
      if (validationConfig == null) None else Some(JSONUtil.deserialize[ValidationConfig](validationConfig)),
      Option(jsonSchema),
      if (denormConfig == null) None else Some(JSONUtil.deserialize[DenormConfig](denormConfig)),
      JSONUtil.deserialize[RouterConfig](routerConfig),
      JSONUtil.deserialize[DatasetConfig](datasetConfig),
      status
    )
  }

  private def parseDatasetTransformation(rs: ResultSet): DatasetTransformation = {
    val id = rs.getString("id")
    val datasetId = rs.getString("dataset_id")
    val fieldKey = rs.getString("field_key")
    val transformationFunction = rs.getString("transformation_function")
    val status = rs.getString("status")

    DatasetTransformation(id, datasetId, fieldKey, JSONUtil.deserialize[TransformationFunction](transformationFunction), status)
  }

}