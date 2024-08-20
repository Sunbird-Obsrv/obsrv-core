package org.sunbird.obsrv.service

import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.obsrv.core.util.{JSONUtil, PostgresConnect, PostgresConnectionConfig}
import org.sunbird.obsrv.model.DatasetModels._
import org.sunbird.obsrv.model.{DatasetStatus, TransformMode}

import java.io.File
import java.sql.{PreparedStatement, ResultSet, Timestamp}

object DatasetRegistryService {
  private val configFile = new File("/data/flink/conf/baseconfig.conf")
  // $COVERAGE-OFF$ This code only executes within a flink cluster
  val config: Config = if (configFile.exists()) {
    println("Loading configuration file cluster baseconfig.conf...")
    ConfigFactory.parseFile(configFile).resolve()
  } else {
    println("Loading configuration file baseconfig.conf inside the jar...")
    ConfigFactory.load("baseconfig.conf").withFallback(ConfigFactory.systemEnvironment())
  }
  // $COVERAGE-ON$
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
    } finally {
      postgresConnect.closeConnection()
    }
  }

  def readDataset(id: String): Option[Dataset] = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      val query = "SELECT * FROM datasets WHERE id = ?"
      preparedStatement = postgresConnect.prepareStatement(query)
      preparedStatement.setString(1, id)
      resultSet = postgresConnect.executeQuery(preparedStatement = preparedStatement)
      if (resultSet.next()) {
        Some(parseDataset(resultSet))
      } else {
        None
      }
    } finally {
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
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
    } finally {
      postgresConnect.closeConnection()
    }
  }


  def readDatasetSourceConfig(datasetId: String): Option[List[DatasetSourceConfig]] = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      val query = "SELECT * FROM dataset_source_config WHERE dataset_id = ?"
      preparedStatement = postgresConnect.prepareStatement(query)
      preparedStatement.setString(1, datasetId)
      resultSet = postgresConnect.executeQuery(preparedStatement = preparedStatement)
      Option(Iterator.continually((resultSet, resultSet.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val datasetSourceConfig = parseDatasetSourceConfig(result)
        datasetSourceConfig
      }).toList)
    } finally {
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
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
    } finally {
      postgresConnect.closeConnection()
    }
  }

  def readDatasources(datasetId: String): Option[List[DataSource]] = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      val query = "SELECT * FROM datasources WHERE dataset_id = ?"
      preparedStatement = postgresConnect.prepareStatement(query)
      preparedStatement.setString(1, datasetId)
      resultSet = postgresConnect.executeQuery(preparedStatement = preparedStatement)
      Option(Iterator.continually((resultSet, resultSet.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        parseDatasource(result)
      }).toList)
    } finally {
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      postgresConnect.closeConnection()
    }
  }

  def readAllDatasources(): Option[List[DataSource]] = {

    val postgresConnect = new PostgresConnect(postgresConfig)
    try {
      val rs = postgresConnect.executeQuery(s"SELECT * FROM datasources")
      Option(Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        parseDatasource(result)
      }).toList)
    }
  }

  def updateDatasourceRef(datasource: DataSource, datasourceRef: String): Int = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    var preparedStatement: PreparedStatement = null
    val query = "UPDATE datasources SET datasource_ref = ? WHERE datasource = ? AND dataset_id = ?"
    try {
      preparedStatement = postgresConnect.prepareStatement(query)
      preparedStatement.setString(1, datasourceRef)
      preparedStatement.setString(2, datasource.datasource)
      preparedStatement.setString(3, datasource.datasetId)
      postgresConnect.executeUpdate(preparedStatement)
    } finally {
      if (preparedStatement != null) preparedStatement.close()
      postgresConnect.closeConnection()
    }
  }
  
  def updateConnectorStats(id: String, lastFetchTimestamp: Timestamp, records: Long): Int = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    var preparedStatement: PreparedStatement = null
    val query = "UPDATE dataset_source_config SET connector_stats = COALESCE(connector_stats, '{}')::jsonb || jsonb_build_object('records', COALESCE(connector_stats->>'records', '0')::int + ? ::int) || jsonb_build_object('last_fetch_timestamp', ? ::timestamp) || jsonb_build_object('last_run_timestamp', ? ::timestamp) WHERE id = ?;"
    try {
      preparedStatement = postgresConnect.prepareStatement(query)
      preparedStatement.setString(1, records.toString)
      preparedStatement.setTimestamp(2, lastFetchTimestamp)
      preparedStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()))
      preparedStatement.setString(4, id)
      postgresConnect.executeUpdate(preparedStatement)
    } finally {
      if (preparedStatement != null) preparedStatement.close()
      postgresConnect.closeConnection()
    }
  }


  def updateConnectorDisconnections(id: String, disconnections: Int): Int = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    var preparedStatement: PreparedStatement = null
    val query = "UPDATE dataset_source_config SET connector_stats = jsonb_set(coalesce(connector_stats, '{}')::jsonb, '{disconnections}', to_jsonb(?)) WHERE id = ?"
    try {
      preparedStatement = postgresConnect.prepareStatement(query)
      preparedStatement.setInt(1, disconnections)
      preparedStatement.setString(2, id)
      postgresConnect.executeUpdate(preparedStatement)
    } finally {
      if (preparedStatement != null) preparedStatement.close()
      postgresConnect.closeConnection()
    }
  }

  def updateConnectorAvgBatchReadTime(id: String, avgReadTime: Long): Int = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    var preparedStatement: PreparedStatement = null
    val query = "UPDATE dataset_source_config SET connector_stats = jsonb_set(coalesce(connector_stats, '{}')::jsonb, '{avg_batch_read_time}', to_jsonb(?)) WHERE id = ?"
    try {
      preparedStatement = postgresConnect.prepareStatement(query)
      preparedStatement.setLong(1, avgReadTime)
      preparedStatement.setString(2, id)
      postgresConnect.executeUpdate(preparedStatement)
    } finally {
      if (preparedStatement != null) preparedStatement.close()
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
    val tagArray = rs.getArray("tags")
    val tags = if (tagArray != null) tagArray.getArray.asInstanceOf[Array[String]] else null
    val dataVersion = rs.getInt("data_version")

    Dataset(datasetId, datasetType,
      if (extractionConfig == null) None else Some(JSONUtil.deserialize[ExtractionConfig](extractionConfig)),
      if (dedupConfig == null) None else Some(JSONUtil.deserialize[DedupConfig](dedupConfig)),
      if (validationConfig == null) None else Some(JSONUtil.deserialize[ValidationConfig](validationConfig)),
      Option(jsonSchema),
      if (denormConfig == null) None else Some(JSONUtil.deserialize[DenormConfig](denormConfig)),
      JSONUtil.deserialize[RouterConfig](routerConfig),
      JSONUtil.deserialize[DatasetConfig](datasetConfig),
      DatasetStatus.withName(status),
      Option(tags),
      Option(dataVersion)
    )
  }

  private def parseDatasetSourceConfig(rs: ResultSet): DatasetSourceConfig = {
    val id = rs.getString("id")
    val datasetId = rs.getString("dataset_id")
    val connectorType = rs.getString("connector_type")
    val connectorConfig = rs.getString("connector_config")
    val connectorStats = rs.getString("connector_stats")
    val status = rs.getString("status")

    DatasetSourceConfig(id = id, datasetId = datasetId, connectorType = connectorType,
      JSONUtil.deserialize[ConnectorConfig](connectorConfig), status,
      if (connectorStats != null) Some(JSONUtil.deserialize[ConnectorStats](connectorStats)) else None
    )
  }

  private def parseDatasource(rs: ResultSet): DataSource = {
    val id = rs.getString("id")
    val datasource = rs.getString("datasource")
    val datasetId = rs.getString("dataset_id")
    val datasourceType = rs.getString("type")
    val datasourceStatus = rs.getString("status")
    val ingestionSpec = rs.getString("ingestion_spec")
    val datasourceRef = rs.getString("datasource_ref")

    DataSource(id, datasource, datasetId, datasourceType, datasourceStatus, ingestionSpec, datasourceRef)
  }

  private def parseDatasetTransformation(rs: ResultSet): DatasetTransformation = {
    val id = rs.getString("id")
    val datasetId = rs.getString("dataset_id")
    val fieldKey = rs.getString("field_key")
    val transformationFunction = rs.getString("transformation_function")
    val status = rs.getString("status")
    val mode = rs.getString("mode")

    DatasetTransformation(id, datasetId, fieldKey, JSONUtil.deserialize[TransformationFunction](transformationFunction), status, Some(if (mode != null) TransformMode.withName(mode) else TransformMode.Strict))
  }

}