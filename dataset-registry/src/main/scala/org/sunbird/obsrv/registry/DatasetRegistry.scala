package org.sunbird.obsrv.registry

import org.sunbird.obsrv.model.DatasetModels.Dataset

object DatasetRegistry {

  var datasets: Map[String, Dataset] = null;

  def getAllDatasets(): Map[String, Dataset] = {

    return Map();
  }

  def initialize(): Unit = {
    // TODO: Move datasets to redis and this initialization can be removed
    datasets = getAllDatasets();
  }
  
  def getDataset(id:String) : Option[Dataset] = {

    return datasets.get(id)
  }

  def getDataSetIds(): List[String] = {
    datasets.keySet.toList
  }



}
