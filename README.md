# obsrv-core

Default Configurations in flink job and dataset registry Settings:

These configurations can be modified as needed to customize the behavior of the pipeline.

## Dataset Registry

Configuration for the Dataset Registry:

| Configuration         | Description                  | Default Value   |
|-----------------------|------------------------------|-----------------|
| postgres.host         | Hostname or IP address       | localhost       |
| postgres.port         | Port number                  | 5432            |
| postgres.maxConnections | Maximum number of connections | 2            |
| postgres.user         | PostgreSQL username          | obsrv           |
| postgres.password     | PostgreSQL password          | obsrv123        |
| postgres.database     | Database name                | obsrv-registry  |

 
## Extractor job

