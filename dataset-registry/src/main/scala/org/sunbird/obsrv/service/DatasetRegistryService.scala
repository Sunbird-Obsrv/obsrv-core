package org.sunbird.obsrv.service

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.core.streaming.BaseDeduplication
import org.sunbird.obsrv.core.util.{JSONUtil, PostgresConnect, PostgresConnectionConfig}
import org.sunbird.obsrv.model.DatasetModels.{ConnectorConfig, DataSource, Dataset, DatasetConfig, DatasetSourceConfig, DatasetTransformation, DedupConfig, DenormConfig, ExtractionConfig, RouterConfig, TransformationFunction, ValidationConfig}

import java.io.File
import java.sql.ResultSet

object DatasetRegistryService {

  private[this] val logger = LoggerFactory.getLogger(DatasetRegistryService.getClass)

  private val configFile = new File("/data/flink/conf/baseconfig.conf")
  val config: Config = if (configFile.exists()) {
    println("Loading configuration file cluster baseconfig.conf...")
    ConfigFactory.parseFile(configFile).resolve()
  } else {
    println("Loading configuration file baseconfig.conf inside the jar...")
    ConfigFactory.load("baseconfig.conf").withFallback(ConfigFactory.systemEnvironment())
  }
  private val postgresConfig = PostgresConnectionConfig(
    config.getString("postgres.user"),
    config.getString("postgres.password"),
    config.getString("postgres.database"),
    config.getString("postgres.host"),
    config.getInt("postgres.port"),
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

  def readAllDatasetSourceConfig(): Option[List[DatasetSourceConfig]] = {

    val postgresConnect = new PostgresConnect(postgresConfig)
    try {
      val rs = postgresConnect.executeQuery("SELECT * FROM dataset_source_config")
      Option(Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val datasetSourceConfig = parseDatasetSourceConfig(result)
        datasetSourceConfig
      }).toList)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        None
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

  def readAllDatasources(): Map[String, List[DataSource]] = {

    val postgresConnect = new PostgresConnect(postgresConfig)
    try {
      val rs = postgresConnect.executeQuery("SELECT * FROM datasources")
      Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val dt = parseDatasource(result)
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

  def updateDatasourceRef(datasource: DataSource, datasourceRef: String): Unit = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    try {
      // TODO: Check if the udpate is successful. Else throw an Exception
      postgresConnect.executeQuery(s"UPDATE datasources set datasource_ref = '$datasourceRef' where datasource='${datasource.datasource}' and dataset_id='${datasource.datasetId}'")
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
    val tags = rs.getArray("tags").getArray.asInstanceOf[Array[String]]

    Dataset(datasetId, datasetType,
      if (extractionConfig == null) None else Some(JSONUtil.deserialize[ExtractionConfig](extractionConfig)),
      if (dedupConfig == null) None else Some(JSONUtil.deserialize[DedupConfig](dedupConfig)),
      if (validationConfig == null) None else Some(JSONUtil.deserialize[ValidationConfig](validationConfig)),
      Option(jsonSchema),
      if (denormConfig == null) None else Some(JSONUtil.deserialize[DenormConfig](denormConfig)),
      JSONUtil.deserialize[RouterConfig](routerConfig),
      JSONUtil.deserialize[DatasetConfig](datasetConfig),
      status,
      Option(tags)
    )
  }

  private def parseDatasetSourceConfig(rs: ResultSet): DatasetSourceConfig = {
    val id = rs.getString("id")
    val datasetId = rs.getString("dataset_id")
    val connectorType = rs.getString("connector_type")
    val connectorConfig = rs.getString("connector_config")
    val status = rs.getString("status")

    DatasetSourceConfig(id = id, datasetId = datasetId, connectorType = connectorType,
      JSONUtil.deserialize[ConnectorConfig](connectorConfig),
      status
    )
  }

  private def parseDatasource(rs: ResultSet): DataSource = {
    val datasource = rs.getString("datasource")
    val datasetId = rs.getString("dataset_id")
    val ingestionSpec = rs.getString("ingestion_spec")
    val datasourceRef = rs.getString("datasource_ref")

    DataSource(datasource, datasetId, ingestionSpec, datasourceRef)
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