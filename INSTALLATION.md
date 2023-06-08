# Obsrv

## Overview

Obsrv comprises several pluggable tools and microservices that come together to enable observability features on any platform/solution. This includes the ability to capture granular events via telemetry, create measures, and observe various events/actions carried out by the system/users/devices (like IoT devices) on any platform/solution. Obsrv comes with a set of microservices, APIs, and some utility SDKs to make it easy for adopters to rapidly enable powerful data processing and aggregation infrastructure to process telemetry data, validate telemetry stream data, as well as aggregate and generate actionable insights via APIs. It also has built-in open data cataloging and publishing capability. It is built keeping extensibility in mind, so that adopters have the flexibility to adapt the telemetry and tools to their specific use-cases.

## Keywords

- Dataset:
In event-driven applications, a dataset is a structured collection of raw data representing specific events. Each event has attributes like timestamp, type, and metadata. Datasets are vital for collecting, transforming, and analyzing data in real-time for various purposes.
- Master Dataset:
A master dataset is a consolidated collection of relevant data from various sources, serving as a unified reference for analysis, decision-making, and reporting. It combines and integrates data from multiple datasets to provide a complete and consistent view. The master dataset is denormalized for improved performance and simplified data access.
- Datasource:
A datasource refers to a specific subset or portion of a dataset that is selected or derived for further processing, analysis, or presentation. It represents a specific source or view of data within the larger dataset. Datasources are created by extracting and manipulating data from the original dataset based on specific criteria, such as filtering, aggregating, or transforming the data. Datasources allow for focused analysis and interpretation of the data within a specific context or for a particular purpose.

## How to setup the obsrv?

The Obsrv Automation repository provides a set of tools and scripts for setting up and configuring Obsrv. Clone the obsrv automation repository from [here](https://github.com/Sunbird-Obsrv/obsrv-automation).

### **Key Words:**

- Terraform: Terraform is an open-source infrastructure provisioning tool that allows for declarative configuration and automation of cloud infrastructure resources.
- S3 Cloud Storage: Amazon S3 (Simple Storage Service) is a scalable and secure cloud storage service offered by AWS, allowing users to store and retrieve data in the form of objects within buckets.

### Prerequisites:

- Install terragrunt. Please see [**Install Terragrunt**](https://terragrunt.gruntwork.io/docs/getting-started/install/) for reference.

**(for aws)**

- You will need key-secret pair to access AWS. Learn how to create or manage these at [Managing access keys for IAM users](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html). Please export these variables in terminal session
    
    `export AWS_ACCESS_KEY_ID=mykey`

    `export AWS_SECRET_ACCESS_KEY=mysecret`
    
- You will require an S3 bucket to store tf-state. Learn how to create or manage these at [Create an Amazon S3 bucket](https://docs.aws.amazon.com/transfer/latest/userguide/requirements-S3.html). Please export this variable at
    
    `export AWS_TERRAFORM_BACKEND_BUCKET_NAME=mybucket`

    `export AWS_TERRAFORM_BACKEND_BUCKET_REGION=myregion` 
    

### Steps:

* In order to complete the installation, please run the below steps in the same terminal.

    `cd terraform/aws`

    `terragrunt init`

    `terragrunt plan`

    `terragrunt apply`

Please refer to the repository's README file for specific instructions on configuring OBSRV on AWS and other cloud providers like GCP and Azure.

## How to create a dataset?

- Assuming that the Obsrv API service is running on localhost:3000 within a cluster, to access the API, you would need to perform a port forwarding operation. This can be achieved using the command: `kubectl port-forward <api-service-name> 3000:3000` . Once done, you can access the Obsrv API service on your local machine at **`localhost:3000`**.
- **Dataset Configurations**
    - **`extraction_config`**: defines how the data is extracted from the source. `is_batch_event`  determines whether the extraction is done in batches or not. The `extraction_key` specifies the key used for extraction.
    - **`validation_config`**: defines the validation rules applied to the dataset. It includes parameters like whether validation is enabled (**`validate`**) and the validation mode (**`mode`**).
    - **`dedup_config`**: handles duplicate records in the dataset. It includes parameters like whether to drop duplicates (**`drop_duplicates`**), the key used for deduplication (**`dedup_key`**), and the deduplication period (**`dedup_period`**) in seconds.
    - **`data_schema`**: Json schema of the data in the dataset.
    - **`denorm_config`**: By denormalizing the user information, the telemetry dataset can become more self-contained and easier to analyze. It eliminates the need for additional queries or joins to retrieve user information when analyzing telemetry data. It has redis config and denorm_fields
    - **`router_config`**: It includes (**`topic`**) to which the dataset is published.

- **Create a master dataset**
    
    **End Point**:`/obsrv/v1/datasets`
    
    **Method**:`POST`
    
    **Request Body:**
    
    ```json
    {"id":"sb-telemetry-user","dataset_id":"sb-telemetry-user","type":"master-dataset","name":"sb-telemetry-user","validation_config":{"validate":true,"mode":"Strict"},"extraction_config":{"is_batch_event":false,"extraction_key":"","dedup_config":{"drop_duplicates":false,"dedup_key":"id","dedup_period":1036800}},"dedup_config":{"drop_duplicates":true,"dedup_key":"id","dedup_period":1036800},"data_schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"subject":{"type":"array","items":{"type":"string"}},"channel":{"type":"string"},"language":{"type":"array","items":{"type":"string"}},"id":{"type":"string"},"firstName":{"type":"string"},"lastName":{"type":"string"},"mobile":{"type":"string"},"email":{"type":"string"},"state":{"type":"string"},"district":{"type":"string"}}},"denorm_config":{"redis_db_host":"obsrv-redis-master.redis.svc.cluster.local","redis_db_port":6379,"denorm_fields":[]},"router_config":{"topic":"user-master"},"dataset_config":{"data_key":"id","timestamp_key":"","exclude_fields":[],"entry_topic":"dev.masterdata.ingest","redis_db_host":"obsrv-redis-master.redis.svc.cluster.local","redis_db_port":6379,"index_data":false,"redis_db":3},"status":"ACTIVE","created_by":"SYSTEM","updated_by":"SYSTEM","published_date":"2023-05-19 05:46:01.854692","tags":[],"data_version":null}
    ```
    
- **Create a dataset with denormalized configurations**
    
    **End Point**:`/obsrv/v1/datasets`
    
    **Method**:`POST`
    
    **Request Body:**
    
    ```json
    {"id":"sb-telemetry","dataset_id":"sb-telemetry","type":"dataset","name":"sb-telemetry","validation_config":{"validate":true,"mode":"Strict","validation_mode":"Strict"},"extraction_config":{"is_batch_event":true,"extraction_key":"events","dedup_config":{"drop_duplicates":true,"dedup_key":"id","dedup_period":1036800},"batch_id":"id"},"dedup_config":{"drop_duplicates":true,"dedup_key":"mid","dedup_period":1036800},"data_schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"eid":{"type":"string"},"ets":{"type":"integer","format":"date-time"},"ver":{"type":"string"},"mid":{"type":"string","oneof":[{"type":"integer"},{"type":"string"}]},"actor":{"type":"object","properties":{"id":{"type":"string"},"type":{"type":"string"}}},"context":{"type":"object","properties":{"channel":{"type":"string"},"pdata":{"type":"object","properties":{"id":{"type":"string"},"ver":{"type":"string"},"pid":{"type":"string"}}},"env":{"type":"string"},"sid":{"type":"string","format":"uuid"},"did":{"type":"string"},"rollup":{"type":"object","properties":{"l1":{"type":"string"}}},"uid":{"type":"string"},"cdata":{"type":"array","additionalProperties":true}}},"object":{"type":"object","properties":{"id":{"type":"string"},"type":{"type":"string"},"ver":{"type":"string"}}},"tags":{"type":"array","items":{"type":"string"}},"edata":{"type":"object","properties":{"type":{"type":"string"},"pageid":{"type":"string"},"subtype":{"type":"string"},"uri":{"type":"string","format":"uri"},"visits":{"type":"array","additionalProperties":true},"level":{"type":"string"},"message":{"type":"string"},"params":{"type":"array","additionalProperties":true},"size":{"type":"integer"},"query":{"type":"string"},"filters":{"type":"object","properties":{"isTenant":{"type":"boolean"},"framework":{"type":"object"},"mimeType":{"type":"object"},"resourceType":{"type":"object"},"subject":{"type":"array","additionalProperties":true},"se_boards":{"type":"array","additionalProperties":true},"se_mediums":{"type":"array","additionalProperties":true},"se_gradeLevels":{"type":"array","additionalProperties":true},"primaryCategory":{"type":"array","additionalProperties":true},"objectType":{"type":"array","additionalProperties":true},"channel":{"type":"array","additionalProperties":true},"contentType":{"type":"array","additionalProperties":true},"visibility":{"type":"array","additionalProperties":true},"batches.status":{"type":"array","items":{"type":"integer"}},"batches.enrollmentType":{"type":"string"},"status":{"type":"array","additionalProperties":true},"migratedVersion":{"type":"integer"},"identifiers":{"type":"array","additionalProperties":true}}},"sort":{"type":"object","properties":{"lastPublishedOn":{"type":"string"}}},"topn":{"type":"array","additionalProperties":true},"props":{"type":"array","additionalProperties":true},"duration":{"type":"integer"},"state":{"type":"string"},"prevstate":{"type":"string"}}},"syncts":{"type":"integer","format":"date-time"},"@timestamp":{"type":"string","format":"date-time"},"flags":{"type":"object","properties":{"ex_processed":{"type":"boolean"}}}},"required":["ets"]},"denorm_config":{"redis_db_host":"obsrv-redis-master.redis.svc.cluster.local","redis_db_port":6379,"denorm_fields":[{"denorm_key":"actor.id","redis_db":3,"denorm_out_field":"user_metadata"}]},"router_config":{"topic":"sb-telemetry"},"dataset_config":{"data_key":"id","timestamp_key":"","exclude_fields":[],"entry_topic":"dev.masterdata.ingest","redis_db_host":"obsrv-redis-master.redis.svc.cluster.local","redis_db_port":6379,"index_data":false,"redis_db":3},"status":"ACTIVE","created_by":"SYSTEM","updated_by":"SYSTEM","created_date":"2023-05-31 12:15:42.845622","updated_date":"2023-05-31 12:15:42.845622","published_date":"2023-05-31 12:15:42.845622","tags":null,"data_version":null}
    ```
    

## How to ingest data?

- First port forward Druid service within the cluster, use the command: **`kubectl port-forward <your-druid-service> 8888:8888`**. Access the service on your local machine at localhost:8888.
- Create ingestion spec, you can refer to the [**official documentation**](https://druid.apache.org/docs/latest/development/extensions-core/kafka-ingestion.html) which provides detailed instructions and examples.
- Create a new data source by deriving it from the previously created dataset.
    
    **End Point**: `/obsrv/v1/datasources`
    
    **Method**: `POST`
    
    **Request Body**: 
    
    ```json
    {"id":"sb-telemetry_sb-telemetry","datasource":"sb-telemetry","dataset_id":"sb-telemetry","ingestion_spec":{"type":"kafka","spec":{"dataSchema":{"dataSource":"sb-telemetry","dimensionsSpec":{"dimensions":[{"type":"string","name":"eid"},{"type":"long","name":"ets"},{"type":"string","name":"ver"},{"type":"string","name":"mid"},{"type":"string","name":"actor_id"},{"type":"string","name":"actor_type"},{"type":"string","name":"context_channel"},{"type":"string","name":"context_pdata_id"},{"type":"string","name":"context_pdata_ver"},{"type":"string","name":"context_pdata_pid"},{"type":"string","name":"context_env"},{"type":"string","name":"context_sid"},{"type":"string","name":"context_did"},{"type":"string","name":"context_rollup_l1"},{"type":"string","name":"context_uid"},{"type":"array","name":"context_cdata"},{"type":"string","name":"object_id"},{"type":"string","name":"object_type"},{"type":"string","name":"object_ver"},{"type":"array","name":"tags"},{"type":"string","name":"edata_type"},{"type":"string","name":"edata_pageid"},{"type":"string","name":"edata_subtype"},{"type":"string","name":"edata_uri"},{"type":"array","name":"edata_visits"},{"type":"string","name":"edata_level"},{"type":"string","name":"edata_message"},{"type":"array","name":"edata_params"},{"type":"string","name":"edata_query"},{"type":"boolean","name":"edata_filters_isTenant"},{"type":"array","name":"edata_filters_subject"},{"type":"array","name":"edata_filters_se_boards"},{"type":"array","name":"edata_filters_se_mediums"},{"type":"array","name":"edata_filters_se_gradeLevels"},{"type":"array","name":"edata_filters_primaryCategory"},{"type":"array","name":"edata_filters_objectType"},{"type":"array","name":"edata_filters_channel"},{"type":"array","name":"edata_filters_contentType"},{"type":"array","name":"edata_filters_visibility"},{"type":"array","name":"edata_filters_batches_status"},{"type":"string","name":"edata_filters_batches_enrollmentType"},{"type":"array","name":"edata_filters_status"},{"type":"array","name":"edata_filters_identifiers"},{"name":"edata_filters_batches"},{"type":"string","name":"edata_sort_lastPublishedOn"},{"type":"array","name":"edata_topn"},{"type":"array","name":"edata_props"},{"type":"string","name":"edata_state"},{"type":"string","name":"edata_prevstate"},{"type":"string","name":"@timestamp"},{"type":"boolean","name":"flags_ex_processed"},{"type":"json","name":"user_metadata"}]},"timestampSpec":{"column":"syncts","format":"auto"},"metricsSpec":[{"type":"doubleSum","name":"edata_size","fieldName":"edata_size"},{"type":"doubleSum","name":"edata_filters_migratedVersion","fieldName":"edata_filters_migratedVersion"},{"type":"doubleSum","name":"edata_duration","fieldName":"edata_duration"}],"granularitySpec":{"type":"uniform","segmentGranularity":"DAY","rollup":false}},"tuningConfig":{"type":"kafka","maxBytesInMemory":134217728,"maxRowsPerSegment":500000,"logParseExceptions":true},"ioConfig":{"type":"kafka","topic":"sb-telemetry","consumerProperties":{"bootstrap.servers":"kafka-headless.kafka.svc:9092"},"taskCount":1,"replicas":1,"taskDuration":"PT1H","useEarliestOffset":true,"completionTimeout":"PT1H","inputFormat":{"type":"json","flattenSpec":{"useFieldDiscovery":true,"fields":[{"type":"path","expr":"$.eid","name":"eid"},{"type":"path","expr":"$.ets","name":"ets"},{"type":"path","expr":"$.ver","name":"ver"},{"type":"path","expr":"$.mid","name":"mid"},{"type":"path","expr":"$.actor.id","name":"actor_id"},{"type":"path","expr":"$.actor.type","name":"actor_type"},{"type":"path","expr":"$.context.channel","name":"context_channel"},{"type":"path","expr":"$.context.pdata.id","name":"context_pdata_id"},{"type":"path","expr":"$.context.pdata.ver","name":"context_pdata_ver"},{"type":"path","expr":"$.context.pdata.pid","name":"context_pdata_pid"},{"type":"path","expr":"$.context.env","name":"context_env"},{"type":"path","expr":"$.context.sid","name":"context_sid"},{"type":"path","expr":"$.context.did","name":"context_did"},{"type":"path","expr":"$.context.rollup.l1","name":"context_rollup_l1"},{"type":"path","expr":"$.context.uid","name":"context_uid"},{"type":"path","expr":"$.context.cdata[*]","name":"context_cdata"},{"type":"path","expr":"$.object.id","name":"object_id"},{"type":"path","expr":"$.object.type","name":"object_type"},{"type":"path","expr":"$.object.ver","name":"object_ver"},{"type":"path","expr":"$.tags[*]","name":"tags"},{"type":"path","expr":"$.edata.type","name":"edata_type"},{"type":"path","expr":"$.edata.pageid","name":"edata_pageid"},{"type":"path","expr":"$.edata.subtype","name":"edata_subtype"},{"type":"path","expr":"$.edata.uri","name":"edata_uri"},{"type":"path","expr":"$.edata.visits[*]","name":"edata_visits"},{"type":"path","expr":"$.edata.level","name":"edata_level"},{"type":"path","expr":"$.edata.message","name":"edata_message"},{"type":"path","expr":"$.edata.params[*]","name":"edata_params"},{"type":"path","expr":"$.edata.query","name":"edata_query"},{"type":"path","expr":"$.edata.filters.isTenant","name":"edata_filters_isTenant"},{"type":"path","expr":"$.edata.filters.subject[*]","name":"edata_filters_subject"},{"type":"path","expr":"$.edata.filters.se_boards[*]","name":"edata_filters_se_boards"},{"type":"path","expr":"$.edata.filters.se_mediums[*]","name":"edata_filters_se_mediums"},{"type":"path","expr":"$.edata.filters.se_gradeLevels[*]","name":"edata_filters_se_gradeLevels"},{"type":"path","expr":"$.edata.filters.primaryCategory[*]","name":"edata_filters_primaryCategory"},{"type":"path","expr":"$.edata.filters.objectType[*]","name":"edata_filters_objectType"},{"type":"path","expr":"$.edata.filters.channel[*]","name":"edata_filters_channel"},{"type":"path","expr":"$.edata.filters.contentType[*]","name":"edata_filters_contentType"},{"type":"path","expr":"$.edata.filters.visibility[*]","name":"edata_filters_visibility"},{"type":"path","expr":"$.edata.filters.batches.status[*]","name":"edata_filters_batches_status"},{"type":"path","expr":"$.edata.filters.batches.enrollmentType","name":"edata_filters_batches_enrollmentType"},{"type":"path","expr":"$.edata.filters.status[*]","name":"edata_filters_status"},{"type":"path","expr":"$.edata.filters.identifiers[*]","name":"edata_filters_identifiers"},{"type":"path","expr":"$.edata.filters.batches","name":"edata_filters_batches"},{"type":"path","expr":"$.edata.sort.lastPublishedOn","name":"edata_sort_lastPublishedOn"},{"type":"path","expr":"$.edata.topn[*]","name":"edata_topn"},{"type":"path","expr":"$.edata.props[*]","name":"edata_props"},{"type":"path","expr":"$.edata.state","name":"edata_state"},{"type":"path","expr":"$.edata.prevstate","name":"edata_prevstate"},{"type":"path","expr":"$.obsrv_meta.syncts","name":"syncts"},{"type":"path","expr":"$.@timestamp","name":"@timestamp"},{"type":"path","expr":"$.flags.ex_processed","name":"flags_ex_processed"},{"type":"path","expr":"$.user_metadata","name":"user_metadata"},{"type":"path","expr":"$.edata.size","name":"edata_size"},{"type":"path","expr":"$.edata.filters.migratedVersion","name":"edata_filters_migratedVersion"},{"type":"path","expr":"$.edata.duration","name":"edata_duration"}]}},"appendToExisting":false}}},"datasource_ref":"sb-telemetry","retention_period":{"enabled":"false"},"archival_policy":{"enabled":"false"},"purge_policy":{"enabled":"false"},"backup_config":{"enabled":"false"},"status":"ACTIVE","created_by":"SYSTEM","updated_by":"SYSTEM","published_date":"2023-05-31 12:15:42.881752"}
    ```
    
- Submit ingestion to Druid
    
    **URL:** `localhost:8888/druid/indexer/v1/supervisor`
    
    **Request Body:** `<ingestion spec from datasource created in above step>`
    
- Push events for the master dataset: Pushing events involves loading data into the master dataset. You can push events through obsrv API using endpoint `/obsrv/v1/data/:datasetId`
    
    **End Point**:`/obsrv/v1/data/sb-telemetry-user`
    
    **Method**:`POST`
    
    **Request Body**:
    
    ```json
    {"data":{"event":{"subject":["Mathematics"],"channel":"Future Assurance Consultant","language":["English"],"id":"user-00","firstName":"Karan","lastName":"Panicker","mobile":"+91-602-8988588","email":"Karan_Panicker@obsrv.ai","state":"Gujarat","district":"Bedfordshire"}}}
    ```
    
- Push events for the normal dataset: Pushing events involves loading data into the normal dataset.
    
    **End Point**:`/obsrv/v1/data/sb-telemetry`
    
    **Method**:`POST`
    
    **Request Body**:
    
    ```json
    {"data":{"id":"dedup-id-1","events":[{"eid":"IMPRESSION","ets":1672657002221,"ver":"3.0","mid":124435,"actor":{"id":"user-00","type":"User"},"context":{"channel":"01268904781886259221","pdata":{"id":"staging.diksha.portal","ver":"5.1.0","pid":"sunbird-portal"},"env":"public","sid":"23850c90-8a8c-11ed-95d0-276800e1048c","did":"0c45959486f579c24854d40a225d6161","cdata":[],"rollup":{"l1":"01268904781886259221"},"uid":"anonymous"},"object":{},"tags":["01268904781886259221"],"edata":{"type":"view","pageid":"login","subtype":"pageexit","uri":"https://staging.sunbirded.org/auth/realms/sunbird/protocol/openid-connect/auth?client_id=portal&state=254efd70-6b89-4f7d-868b-5c957f54174e&redirect_uri=https%253A%252F%252Fstaging.sunbirded.org%252Fresources%253Fboard%253DState%252520(Andhra%252520Pradesh)%2526medium%253DEnglish%2526gradeLevel%253DClass%2525201%2526%2526id%253Dap_k-12_1%2526selectedTab%253Dhome%2526auth_callback%253D1&scope=openid&response_type=code&version=4","visits":[]},"syncts":1672657005814,"@timestamp":"2023-01-02T10:56:45.814Z","flags":{"ex_processed":true}}]}}
    ```
    
     
    

## How to query on data source?

- You can use Obsrv API for druid native and sql queries.
    
    **For native query:**
    
    **End Point**:`/obsrv/v1/query`
    
    **Method**:`POST`
    
    **Request Body**:
    
    ```json
    {"context":{"dataSource":"sb-telemetry"},"query":{"queryType":"scan","dataSource":"sb-telemetry","intervals":"2023-03-31/2023-04-01","granularity":"DAY"}}
    ```
    
    **For SQL query:**
    
    **End Point**:`/obsrv/v1/sql-query`
    
    **Method**:`POST`
    
    **Request Body**:
    
    ```json
    {"context":{"dataSource":"sb-telemetry"},"querySql":"YOUR QUERY STRING"}
    ```
    
For more info on Obsrv API Service refer [**here**](https://github.com/Sunbird-Obsrv/obsrv-api-service/tree/main/swagger-doc)
    