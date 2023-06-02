package org.sunbird.obsrv.spec

import org.scalatest.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.obsrv.registry.DatasetRegistry

class TestDatasetRegistrySpec extends BaseSpecWithDatasetRegistry with Matchers with MockitoSugar {

  "TestDatasetRegistrySpec" should "validate all the registry service methods" in {

    val d1Opt = DatasetRegistry.getDataset("d1")
    d1Opt should not be (None)
    d1Opt.get.id should be ("d1")
    d1Opt.get.dataVersion.get should be (2)

    val d2Opt = DatasetRegistry.getDataset("d2")
    d2Opt should not be (None)
    d2Opt.get.id should be ("d2")
    d2Opt.get.denormConfig should be (None)

    val allDatasets = DatasetRegistry.getAllDatasets("dataset")
    allDatasets.size should be (2)

    val d1Tfs = DatasetRegistry.getDatasetTransformations("d1")
    d1Tfs should not be (None)
    d1Tfs.get.size should be (2)

    val ids = DatasetRegistry.getDataSetIds("dataset")
    ids.head should be ("d1")
    ids.last should be ("d2")
  }

}
