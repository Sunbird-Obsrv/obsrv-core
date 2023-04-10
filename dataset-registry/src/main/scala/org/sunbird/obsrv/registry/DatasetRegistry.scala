package org.sunbird.obsrv.registry

import org.sunbird.obsrv.model.DatasetModels.{Dataset, DatasetSourceConfig, DatasetTransformation}
import org.sunbird.obsrv.service.DatasetRegistryService

object DatasetRegistry {

  private val datasets: Map[String, Dataset] = DatasetRegistryService.readAllDatasets()
  private val datasetTransformations: Map[String, List[DatasetTransformation]] = DatasetRegistryService.readAllDatasetTransformations()
  private val datasetSourceConfig: Option[List[DatasetSourceConfig]] = DatasetRegistryService.readAllDatasetSourceConfig()

  def getAllDatasets(): List[Dataset] = {
    datasets.values.toList
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

  def getDataSetIds(): List[String] = {
    datasets.keySet.toList
  }

}