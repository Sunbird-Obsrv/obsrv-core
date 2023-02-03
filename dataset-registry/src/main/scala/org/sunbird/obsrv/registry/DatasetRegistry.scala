package org.sunbird.obsrv.registry

import org.sunbird.obsrv.model.DatasetModels.{Dataset, DedupConfig, DenormConfig, ExtractionConfig, RouterConfig, ValidationConfig}
import com.typesafe.config.ConfigFactory
import org.sunbird.obsrv.core.util.{JSONUtil, PostgresConnect, PostgresConnectionConfig}

import java.sql.ResultSet

object DatasetRegistry {

  private val datasets: Map[String, Dataset] = findAllDatasets();

  def getAllDatasets(): List[Dataset] = {
    datasets.values.toList;
  }

  private def findAllDatasets(): Map[String, Dataset] = {
    val config = ConfigFactory.load("base-config.conf")
    val postgresConfig = PostgresConnectionConfig(config.getString("postgres.user"), config.getString("postgres.password"),
      config.getString("postgres.database"), config.getString("postgres.host"), config.getInt("postgres.port"),
      config.getInt("postgres.maxConnections"))

    val postgresConnect = new PostgresConnect(postgresConfig);
    try {
      val rs = postgresConnect.executeQuery("SELECT * FROM DATASETS")
      Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val dataset = parseDataset(rs)
        (dataset.id, dataset)
      }).toMap
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        Map()
    } finally {
      postgresConnect.closeConnection()
    }
  }


  private def parseDataset(rs: ResultSet): Dataset = {
    val datasetId = rs.getString("id")
    val validationConfig = rs.getString("validation_config")
    val extractionConfig = rs.getString("extraction_config")
    val dedupConfig = rs.getString("dedup_config")
    val jsonSchema = rs.getString("json_schema")
    val denormConfig = rs.getString("denorm_config")
    val routerConfig = rs.getString("router_config")

    Dataset(datasetId,
      if(extractionConfig == null)  None else Some(JSONUtil.deserialize[ExtractionConfig](extractionConfig)),
      if(dedupConfig == null)  None else Some(JSONUtil.deserialize[DedupConfig](dedupConfig)),
      if(validationConfig == null)  None else Some(JSONUtil.deserialize[ValidationConfig](validationConfig)),
      if(jsonSchema == null)  None else Some(jsonSchema),
      if(denormConfig == null)  None else Some(JSONUtil.deserialize[DenormConfig](denormConfig)),
      JSONUtil.deserialize[RouterConfig](routerConfig)
    )
  }
  
  def getDataset(id:String) : Option[Dataset] = {
    return datasets.get(id)
  }

  def getDataSetIds(): List[String] = {
    datasets.keySet.toList
  }

  def getRouterConfigs(): List[RouterConfig] = {
    datasets.map(f => f._2.routerConfig).toList
  }

}

object TestDatasetRegistry {
  def main(args: Array[String]): Unit = {
    Console.println(DatasetRegistry.getDataset("obs2.0").get.jsonSchema.get)
  }
}