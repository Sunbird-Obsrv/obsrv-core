package org.sunbird.obsrv.spec

import org.scalatest.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.sunbird.obsrv.core.util.PostgresConnect
import org.sunbird.obsrv.registry.DatasetRegistry

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

class TestDatasetRegistrySpec extends BaseSpecWithDatasetRegistry with Matchers with MockitoSugar {

  "TestDatasetRegistrySpec" should "validate all the registry service methods" in {

    val d1Opt = DatasetRegistry.getDataset("d1")
    d1Opt should not be None
    d1Opt.get.id should be("d1")
    d1Opt.get.dataVersion.get should be(2)

    val d2Opt = DatasetRegistry.getDataset("d2")
    d2Opt should not be None
    d2Opt.get.id should be("d2")
    d2Opt.get.denormConfig should be(None)

    val postgresConnect = new PostgresConnect(postgresConfig)
    postgresConnect.execute("insert into datasets(id, type, data_schema, router_config, dataset_config, status, created_by, updated_by, created_date, updated_date, tags) values ('d3', 'dataset', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"topic\":\"d2-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\"}', 'Live', 'System', 'System', now(), now(), ARRAY['Tag1','Tag2']);")
    postgresConnect.closeConnection()

    val d3Opt = DatasetRegistry.getDataset("d3")
    d3Opt should not be None
    d3Opt.get.id should be("d3")
    d3Opt.get.denormConfig should be(None)

    val d4Opt = DatasetRegistry.getDataset("d4")
    d4Opt should be (None)

    val allDatasets = DatasetRegistry.getAllDatasets("dataset")
    allDatasets.size should be(3)

    val d1Tfs = DatasetRegistry.getDatasetTransformations("d1")
    d1Tfs should not be None
    d1Tfs.get.size should be(2)

    val ids = DatasetRegistry.getDataSetIds("dataset").sortBy(f => f)
    ids.head should be("d1")
    ids.apply(1) should be("d2")
    ids.apply(2) should be("d3")

    DatasetRegistry.getAllDatasetSourceConfig().get.size should be(2)
    val datasetSourceConfigList = DatasetRegistry.getDatasetSourceConfigById("d1").get
    val datasetSourceConfig = datasetSourceConfigList.filter(f => f.id.equals("sc1")).head
    datasetSourceConfig.id should be("sc1")
    datasetSourceConfig.datasetId should be("d1")
    datasetSourceConfig.connectorType should be("kafka")
    datasetSourceConfig.status should be("Live")

    val instant1 = LocalDateTime.now(ZoneOffset.UTC)
    DatasetRegistry.updateConnectorStats("sc1", Timestamp.valueOf(instant1), 20L)
    DatasetRegistry.updateConnectorDisconnections("sc1", 2)
    DatasetRegistry.updateConnectorDisconnections("sc1", 4)
    DatasetRegistry.updateConnectorAvgBatchReadTime("sc1", 4)
    DatasetRegistry.updateConnectorAvgBatchReadTime("sc1", 5)
    val instant2 = LocalDateTime.now(ZoneOffset.UTC)

    DatasetRegistry.updateConnectorStats("sc1", Timestamp.valueOf(instant2), 60L)
    val datasetSourceConfigList2 = DatasetRegistry.getDatasetSourceConfigById("d1").get
    val datasetSourceConfig2 = datasetSourceConfigList2.filter(f => f.id.equals("sc1")).head
    datasetSourceConfig2.connectorStats.get.records should be(80)
    datasetSourceConfig2.connectorStats.get.disconnections should be(4)
    datasetSourceConfig2.connectorStats.get.avgBatchReadTime should be(5)
    datasetSourceConfig2.connectorStats.get.lastFetchTimestamp.getTime should be(instant2.toInstant(ZoneOffset.UTC).toEpochMilli)

    val datasource = DatasetRegistry.getDatasources("d1").get.head
    datasource.datasetId should be("d1")
    datasource.datasource should be("d1-datasource")
    datasource.datasourceRef should be("d1-datasource-1")

    DatasetRegistry.updateDatasourceRef(datasource, "d1-datasource-2")
    val datasource2 = DatasetRegistry.getDatasources("d1").get.head
    datasource2.datasourceRef should be("d1-datasource-2")

    DatasetRegistry.getDatasources("d2").get.nonEmpty should be(false)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareTestData()
  }

  private def prepareTestData(): Unit = {
    val postgresConnect = new PostgresConnect(postgresConfig)
    postgresConnect.execute("insert into dataset_source_config values('sc1', 'd1', 'kafka', '{\"kafkaBrokers\":\"localhost:9090\",\"topic\":\"test-topic\"}', 'Live', null, 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_source_config values('sc2', 'd1',  'rdbms', '{\"type\":\"postgres\",\"tableName\":\"test-table\"}', 'Live', null, 'System', 'System', now(), now());")

    //postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasources ( id text PRIMARY KEY, dataset_id text REFERENCES datasets (id), ingestion_spec json NOT NULL, datasource text NOT NULL, datasource_ref text NOT NULL, retention_period json, archival_policy json, purge_policy json, backup_config json NOT NULL, status text NOT NULL, created_by text NOT NULL, updated_by text NOT NULL, created_date Date NOT NULL, updated_date Date NOT NULL );")
    postgresConnect.execute("insert into datasources values('ds1', 'd1',  '{}', 'd1-datasource', 'd1-datasource-1', null, null, null, '{}', 'Live', 'System', 'System', now(), now());")
    postgresConnect.closeConnection()
  }
}