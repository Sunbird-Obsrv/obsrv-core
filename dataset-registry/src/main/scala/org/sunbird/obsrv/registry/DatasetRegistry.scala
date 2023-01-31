package org.sunbird.obsrv.registry

import org.sunbird.obsrv.model.DatasetModels.{Dataset, RouterConfig}

object DatasetRegistry {

  private val datasets: Map[String, Dataset] = findAllDatasets();

  def getAllDatasets(): List[Dataset] = {
    datasets.values.toList;
  }

  private def findAllDatasets(): Map[String, Dataset] = {
    // Todo query postgres to get the datasets
    Map()
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
