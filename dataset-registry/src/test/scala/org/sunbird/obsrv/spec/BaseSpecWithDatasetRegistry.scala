package org.sunbird.obsrv.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.obsrv.core.util.{PostgresConnect, PostgresConnectionConfig}
import org.sunbird.spec.BaseSpecWithPostgres

class BaseSpecWithDatasetRegistry extends BaseSpecWithPostgres {


  val config: Config = ConfigFactory.load("test.conf")
  val postgresConfig = PostgresConnectionConfig(
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"),
    database = "postgres",
    host = config.getString("postgres.host"),
    port = config.getInt("postgres.port"),
    maxConnections = config.getInt("postgres.maxConnections")
  )

  override def beforeAll() {
    super.beforeAll()
    val postgresConnect = new PostgresConnect(postgresConfig)
    createSchema(postgresConnect)
    insertTestData(postgresConnect)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  private def createSchema(postgresConnect: PostgresConnect) {

    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasets ( id text PRIMARY KEY, validation_config json, extraction_config json, dedup_config json, data_schema json, denorm_config json, router_config json NOT NULL, status text NOT NULL, created_by text NOT NULL, updated_by text NOT NULL, created_date timestamp NOT NULL, updated_date timestamp NOT NULL );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasources ( id text PRIMARY KEY, dataset_id text REFERENCES datasets (id), ingestion_spec json NOT NULL, datasource text NOT NULL, retention_period json, archival_policy json, purge_policy json, backup_config json NOT NULL, status text NOT NULL, created_by text NOT NULL, updated_by text NOT NULL, created_date Date NOT NULL, updated_date Date NOT NULL );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS dataset_transformations ( id text PRIMARY KEY, dataset_id text REFERENCES datasets (id), field_key text NOT NULL, transformation_function text NOT NULL, status text NOT NULL, created_by text NOT NULL, updated_by text NOT NULL, created_date Date NOT NULL, updated_date Date NOT NULL, UNIQUE(field_key, dataset_id) );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasets_denorm_config ( id SERIAL PRIMARY KEY, field_key text NOT NULL, dataset_id text NOT NULL REFERENCES datasets (id), connector_type text NOT NULL, connector_config json NOT NULL, data_schema json NOT NULL, field_out_key text NOT NULL, exclude_fields text[], field_transformations json, status text NOT NULL, created_by text NOT NULL, updated_by text NOT NULL, created_date Date NOT NULL, updated_date Date NOT NULL, UNIQUE(field_key, dataset_id) );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasets_source_config ( id SERIAL PRIMARY KEY, dataset_id text NOT NULL REFERENCES datasets (id), connector_type text NOT NULL, connector_config json NOT NULL, status text NOT NULL, created_by text NOT NULL, updated_by text NOT NULL, created_date Date NOT NULL, updated_date Date NOT NULL, UNIQUE(dataset_id) );")
  }

  private def insertTestData(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("insert into datasets(id, data_schema, validation_config, extraction_config, dedup_config, router_config, status, created_by, updated_by, created_date, updated_date) values ('d1', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"validate\": true, \"mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}}', '{\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}', '{\"topic\":\"d1-events\"}', 'ACTIVE', 'System', 'System', now(), now());")
    postgresConnect.execute("update datasets set denorm_config = '{\"redis_db_host\":\"localhost\",\"redis_db_port\":\"6340\",\"denorm_fields\":[{\"denorm_key\":\"vehicleCode\",\"redis_db\":2,\"denorm_out_field\":\"vehicleData\"}]}' where id='d1';")
    postgresConnect.execute("insert into dataset_transformations values('tf1', 'd1', 'dealer.email', '{\"type\":\"mask\",\"expr\":\"dealer.email\"}', 'active', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into dataset_transformations values('tf2', 'd1', 'dealer.maskedPhone', '{\"type\":\"mask\",\"expr\": \"dealer.phone\"}', 'active', 'System', 'System', now(), now());")
    postgresConnect.execute("insert into datasets(id, data_schema, router_config, status, created_by, updated_by, created_date, updated_date) values ('d2', '{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"id\":\"https://sunbird.obsrv.com/test.json\",\"title\":\"Test Schema\",\"description\":\"Test Schema\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"vehicleCode\":{\"type\":\"string\"},\"date\":{\"type\":\"string\"},\"dealer\":{\"type\":\"object\",\"properties\":{\"dealerCode\":{\"type\":\"string\"},\"locationId\":{\"type\":\"string\"},\"email\":{\"type\":\"string\"},\"phone\":{\"type\":\"string\"}},\"required\":[\"dealerCode\",\"locationId\"]},\"metrics\":{\"type\":\"object\",\"properties\":{\"bookingsTaken\":{\"type\":\"number\"},\"deliveriesPromised\":{\"type\":\"number\"},\"deliveriesDone\":{\"type\":\"number\"}}}},\"required\":[\"id\",\"vehicleCode\",\"date\",\"dealer\",\"metrics\"]}', '{\"topic\":\"d1-events\"}', 'ACTIVE', 'System', 'System', now(), now());")
  }

}
