package org.sunbird.obsrv.registry

import org.sunbird.obsrv.model.DatasetModels.{DataSource, Dataset, DatasetSourceConfig, DatasetTransformation}
import org.sunbird.obsrv.service.DatasetRegistryService

object DatasetRegistry {

  private val datasets: Map[String, Dataset] = DatasetRegistryService.readAllDatasets()
  private val datasetTransformations: Map[String, List[DatasetTransformation]] = DatasetRegistryService.readAllDatasetTransformations()
  private val datasetSourceConfig: Option[List[DatasetSourceConfig]] = DatasetRegistryService.readAllDatasetSourceConfig()
  private val datasources: Map[String, List[DataSource]] = DatasetRegistryService.readAllDatasources()

  def getAllDatasets(datasetType: String): List[Dataset] = {
    datasets.filter(f => f._2.datasetType.equals(datasetType)).values.toList
  }

  def getDataset(id: String): Option[Dataset] = {
    datasets.get(id)
  }

  def getDatasetSourceConfig(): Option[List[DatasetSourceConfig]] = {
    datasetSourceConfig
  }

  def getDatasetTransformations(id: String): Option[List[DatasetTransformation]] = {
    datasetTransformations.get(id)
  }

  def getDatasources(datasetId: String): Option[List[DataSource]] = {
    datasources.get(datasetId)
  }

  def getDataSetIds(datasetType: String): List[String] = {
    datasets.filter(f => f._2.datasetType.equals(datasetType)).keySet.toList
  }

  def updateDatasourceRef(datasource: DataSource, datasourceRef: String): Unit = {
    DatasetRegistryService.updateDatasourceRef(datasource, datasourceRef)
  }

}