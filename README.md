# Obsrv core


## Overview 

Obsrv-core is a framework consisting of Flink jobs designed to handle data extraction and processing tasks efficiently. It provides a flexible and customizable pipeline for various data-related operations. These jobs have been designed to process, enrich, and validate data from various sources, making them highly adaptable to a wide range of datasets. The data streaming jobs are built with a generic approach that makes them robust and able to handle diverse datasets without requiring significant changes to the underlying code.
## Default Configurations in flink job and dataset registry Settings:

Please note that these configurations can be modified as needed to customize the behavior of the pipeline.

### Indices

 * [Common config](#common-configuration)
 * [Dataset Registry config](#dataset-registry)
 * [Extraction Job config](#extractor-job)
 * [Preprocessor Job config](#preprocessor-job)
 * [Denorm Job config](#denormalizer-job)
 * [Router Job config](#router-job)
 * [Kafka Connector Job config](#kafka-connector-job)
 * [Masterdata Processor Job config](#masterdata-processor-job)

## Common Configuration

| Configuration                          |Description                                                                  |Data Type| Default Value                 |
|--------------------------------------------|-------|---------------------------------------------------------------------|-------------------------------|
| kafka.consumer.broker-servers                      | Kafka broker servers for the consumer | string                                            | localhost:9092                |
| kafka.producer.broker-servers                      | Kafka broker servers for the producer| string                                             | localhost:9092                |
| kafka.producer.max-request-size                    | Maximum request size for the Kafka producer in bytes  | number                            | 1572864                       |
| kafka.producer.batch.size                          | Batch size for the Kafka producer in bytes  | number                                      | 98304                         |
| kafka.producer.linger.ms                           | Linger time in milliseconds for the Kafka producer      | number                          | 10                            |
| kafka.producer.compression                         | Compression type for the Kafka producer    | string                                        | snappy                        |
| kafka.output.system.event.topic                    | Output Kafka topic for system events    | string                                          | local.system.events           |
| job.env                                            | Environment for the Flink job | string                                                     | local                         |
| job.enable.distributed.checkpointing               | Flag indicating whether distributed checkpointing is enabled for the job  |boolean        | false                         |
| job.statebackend.blob.storage.account              | Blob storage account for the state backend  | string                                      | blob.storage.account          |
| job.statebackend.blob.storage.container            | Blob storage container for the state backend | string                                     | obsrv-container               |
| job.statebackend.blob.storage.checkpointing.dir    | Directory for checkpointing in the blob storage  | string                                 | flink-jobs                    |
| job.statebackend.base.url                          | Base URL for the state backend      |string    url                                            | wasbs://obsrv-container@blob.storage.account/flink-jobs |
| task.checkpointing.compressed                      | Flag indicating whether checkpointing is compressed |boolean                              | true                          |
| task.checkpointing.interval                        | Interval between checkpoints in milliseconds |number                                    | 60000                         |
| task.checkpointing.pause.between.seconds           | Pause between checkpoints in seconds |number                                             | 30000                         |
| task.restart-strategy.attempts                     | Number of restart attempts for the job|number                                            | 3                             |
| task.restart-strategy.delay                        | Delay between restart attempts in milliseconds |number                                   | 30000                         |
| task.parallelism                                   | Parallelism for the Flink job tasks|number                                               | 1                             |
| task.consumer.parallelism                          | Parallelism for the task consumers  |number                                              | 1                             |
| task.downstream.operators.parallelism              | Parallelism for downstream operators |number                                             | 1                             |
| redis.host                                         | Hostname of the Redis server| string                                                       | localhost                     |
| redis.port                                         | Port number of the Redis server| number                                                    | 6379                          |
| redis.connection.timeout                           | Connection timeout for Redis in milliseconds |number                                     | 30000                         |
| redis-meta.host                                    | Hostname of the Redis server for metadata |string                                         | localhost                     |
| redis-meta.port                                    | Port number of the Redis server for metadata   |number                                    | 6379                          |
| postgres.host                                      | Hostname or IP address of the PostgreSQL server  |string                                 | localhost                     |
| postgres.port                                      | Port number of the PostgreSQL server |number                                             | 5432                          |
| postgres.maxConnections                            | Maximum number of connections to the PostgreSQL server|number                            | 2                             |
| postgres.user                                      | PostgreSQL username | string                                                             | postgres                      |
| postgres.password                                  | PostgreSQL password  |string                                                            | postgres                      |
| postgres.database                                  | Name of the PostgreSQL database   |string                                                | postgres                      |

 
## Dataset Registry

| Configuration         | Description                  |Data type| Default Value   |
|-----------------------|-----------------------------|----------|-----------------|
| postgres.host         | Hostname or IP address       |string| localhost       |
| postgres.port         | Port number                  |number| 5432            |
| postgres.maxConnections | Maximum number of connections |number| 2            |
| postgres.user         | PostgreSQL username         |string | obsrv           |
| postgres.password     | PostgreSQL password          |string| obsrv123        |
| postgres.database     | Database name                |string| obsrv-registry  |
 
## Extractor Job

| Configuration             | Description                      |Data type| Default Value   |
|---------------------------|----------------------------------|---------|-----------------|
| kafka.input.topic         | Input Kafka topic                |string| local.ingest |
| kafka.output.raw.topic    | Output Kafka topic for raw data  |string| local.raw |
| kafka.output.extractor.duplicate.topic | Output Kafka topic for duplicate data in extractor |string| local.extractor.duplicate |
| kafka.output.failed.topic | Output Kafka topic for failed data |string| local.failed |
| kafka.output.batch.failed.topic | Output Kafka topic for failed extractor batches |string| local.extractor.failed |
| kafka.event.max.size      | Maximum size of a Kafka event    |string| "1048576" (1MB) |
| kafka.groupId             | Kafka consumer group ID          |string| local-extractor-group |
| kafka.producer.max-request-size | Maximum request size for Kafka producer |number| 5242880 |
| task.consumer.parallelism | Parallelism for task consumers   |number| 1               |
| task.downstream.operators.parallelism | Parallelism for downstream operators |number| 1 |
| redis.database.extractor.duplication.store.id | Redis database ID for extractor duplication store |number| 1 |
| redis.database.key.expiry.seconds | Expiry time for Redis keys (in seconds) |number| 3600 |

## Preprocessor Job

| Configuration             | Description                      |Data type| Default Value   |
|---------------------------|----------------------------------|----------|-----------------|
| kafka.input.topic         | Input Kafka topic                |string| local.raw |
| kafka.output.failed.topic | Output Kafka topic for failed data |string| local.failed |
| kafka.output.invalid.topic | Output Kafka topic for invalid data |string| local.invalid |
| kafka.output.unique.topic | Output Kafka topic for unique data |string| local.unique |
| kafka.output.duplicate.topic | Output Kafka topic for duplicate data |string| local.duplicate |
| kafka.groupId             | Kafka consumer group ID          |string| local-pipeline-preprocessor-group |
| task.consumer.parallelism | Parallelism for task consumers   |number| 1               |
| task.downstream.operators.parallelism | Parallelism for downstream operators |number| 1 |
| redis.database.preprocessor.duplication.store.id | Redis database ID for preprocessor duplication store |number| 2 |
| redis.database.key.expiry.seconds | Expiry time for Redis keys (in seconds) |number| 3600 |

## Denormalizer Job
 
| Configuration                      | Description                                         |Data type| Default Value          |
|------------------------------------|------------------------------------------------------|----|------------------------|
| kafka.input.topic                  | Input Kafka topic                                    |string| local.unique           |
| kafka.output.denorm.topic          | Output Kafka topic for denormalized data              |string| local.denorm           |
| kafka.output.denorm.failed.topic   | Output Kafka topic for failed denormalization         |string| local.denorm.failed    |
| kafka.groupId                      | Kafka consumer group ID                              |string| local-denormalizer-group |
| task.window.time.in.seconds        | Time duration for window in seconds                   |number| 5                      |
| task.window.count                  | configuration specifies the number of events (elements) that will be included in each window. It determines the size of each window for processing. |number| 30                     |
| task.window.shards                 | determines the number of parallel shards (instances) used for processing windows. It enables parallel processing of windows for improved scalability and performance.       |number| 1400                   |
| task.consumer.parallelism          | Parallelism for task consumers                        |number| 1                      |
| task.downstream.operators.parallelism | Parallelism for downstream operators               |number| 1                      |

## Router Job
 
| Configuration          | Description                                  |Data type| Default Value            |
|------------------------|----------------------------------------------|----|--------------------------|
| kafka.input.topic      | Input Kafka topic                            |string| local.transform          |
| kafka.stats.topic      | Kafka topic for storing statistics           |string| local.stats              |
| kafka.groupId          | Kafka consumer group ID                      |string| local-druid-router-group |
| task.consumer.parallelism | Parallelism for task consumers             |number| 1                        |
| task.downstream.operators.parallelism | Parallelism for downstream operators   |number| 1                        |

## Kafka connector Job

| Configuration                      | Description                                        |Data type| Default Value                  |
|------------------------------------|----------------------------------------------------|----|--------------------------------|
| kafka.input.topic                  | Input Kafka topic                                  |string| local.input                     |
| kafka.output.failed.topic          | Output Kafka topic for failed data                 |string| local.failed                   |
| kafka.event.max.size               | Maximum size of events in bytes                    |number| 1048576 (1MB)                  |
| kafka.groupId                      | Kafka consumer group ID                            |string| local-kafkaconnector-group     |
| kafka.producer.max-request-size    | Maximum request size for Kafka producer in bytes   |number| 5242880 (5MB)                  |
| task.consumer.parallelism          | Parallelism for task consumers                     |number| 1                              |
| task.downstream.operators.parallelism | Parallelism for downstream operators             |number|1                              |

## MasterData Processor Job

| Configuration                             | Description                                        | Data Type | Default Value                  |
|-------------------------------------------|----------------------------------------------------|-----------|--------------------------------|
| master-data-processor.kafka.input.topic     | Input Kafka topic                                  | String    | local.masterdata.ingest |
| master-data-processor.kafka.output.raw.topic | Output Kafka topic for raw data                    | String    | local.masterdata.raw |
| master-data-processor.kafka.output.extractor.duplicate.topic | Output Kafka topic for duplicate data extraction | String    | local.masterdata.extractor.duplicate |
| master-data-processor.kafka.output.failed.topic | Output Kafka topic for failed data               | String    | local.masterdata.failed |
| master-data-processor.kafka.output.batch.failed.topic | Output Kafka topic for batch extraction failures | String    | local.masterdata.extractor.failed |
| master-data-processor.kafka.event.max.size | Maximum size of events in bytes                    | Number    | 1048576 (1MB) |
| master-data-processor.kafka.output.invalid.topic | Output Kafka topic for invalid data             | String    | local.masterdata.invalid |
| master-data-processor.kafka.output.unique.topic | Output Kafka topic for unique data               | String    | local.masterdata.unique |
| master-data-processor.kafka.output.duplicate.topic | Output Kafka topic for duplicate data           | String    | local.masterdata.duplicate |
| master-data-processor.kafka.output.transform.topic | Output Kafka topic for transformed data         | String    | local.masterdata.transform |
| master-data-processor.kafka.stats.topic       | Kafka topic for statistics data                    | String    | local.masterdata.stats |
| master-data-processor.kafka.groupId           | Kafka consumer group ID                            | String    | local-masterdata-pipeline-group |
| master-data-processor.kafka.producer.max-request-size | Maximum request size for Kafka producer      | Number    | 5242880 (5MB) |
| master-data-processor.task.window.time.in.seconds | Time window in seconds for tasks              | Number    | 5 |
| master-data-processor.task.window.count     | Count of events within the time window              | Number    | 30 |
| master-data-processor.task.window.shards    | Number of shards for the time window                | Number    | 1400 |
| master-data-processor.task.consumer.parallelism | Parallelism for task consumers                  | Number    | 1 |
| master-data-processor.task.downstream.operators.parallelism | Parallelism for downstream operators    | Number    | 1 |
| master-data-processor.redis.database.extractor.duplication.store.id | Redis store ID for extractor duplication  | Number    | 1 |
| master-data-processor.redis.database.preprocessor.duplication.store.id | Redis store ID for preprocessor duplication | Number    | 2 |
| master-data-processor.redis.database.key.expiry.seconds | Expiry time for Redis keys in seconds       | Number    | 3600 |
| master-data-processor.dataset.type           | Type of master dataset                                    | String    | master-dataset |
