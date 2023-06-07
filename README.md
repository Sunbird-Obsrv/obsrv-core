# Obsrv Core Service
To enable creation, configuration, ingestion and querying of data over OBSRV, following APIs are made available. The folowing concepts are used:

## Dataset
A dataset is an entity that stores the data. There are two types of Datasets:
1. Dataset: This entity holds your main data. This entity will be reularly updated from it's source and you can run your analytics on top of it.
2. Master Dataset: A Master Dataset holds your denorm data. This entity is not updated as regularly and not indexed into the analytical store.  
Both types of Datasets will have a DataSource.

## Datasource
A datasource is an entity which holds information regarding the source of truth for your data.  

## Dataset APIs
The following CRUL APIs are enabled for Dataset:
### Create
**End Point**: `/obsrv/v1/datasets`  
**Method**: `POST`  
**Body Schema**: 
```
description: dataset_id, type, router_config, published_date are required
type: object
properties:
  id:
    type: string
  dataset_id:
    type: string
  name:
    type: string
  type:
    type: string
    enum:
      - dataset
      - master-dataset
  extraction_config:
    type: object
    properties:
      is_batch_event:
        type: boolean
      extraction_key:
        type: string
  validation_config:
    type: object
    properties:
      validate:
        type: boolean
      mode:
        type: string
  dedup_config:
    type: object
    properties:
      drop_duplicates:
        type: boolean
      dedup_key:
        type: string
      dedup_period:
        type: integer
  data_schema:
    type: object
    properties:
      type:
        type: string
  denorm_config:
    type: object
    properties:
      redis_db_host:
        type: string
      redis_db_port:
        type: string
      denorm_fields:
        type: array
        items:
          type: string
          properties:
            denorm_key:
              type: string
            redis_db:
              type: integer
            denorm_out_field:
              type: string
  router_config:
    type: object
    properties:
      topic:
        type: string
    required:
      - topic
  tags:
    type: array
    items:
      type: string
  status:
    type: string
    enum:
      - ACTIVE
      - DISABLED
  created_by:
    type: string
  updated_by:
    type: string
  published_date:
    type: string
```
### Read
**End Point**: `/obsrv/v1/datasets/{datasetId}`  
**Method**: `GET`  
**Params**: 
```
name: datasetId
in: path
required: true
schema:
  type: string
  format: uuid
```
### Update
**End Point**: `/obsrv/v1/datasets`  
**Method**: `PATCH`  
**Body Schema**: 
```
description: dataset_id is required
type: object
properties:
  id:
    type: string
  dataset_id:
    type: string
  name:
    type: string
  type:
    type: string
    enum:
      - dataset
      - master-dataset
  extraction_config:
    type: object
    properties:
      is_batch_event:
        type: boolean
      extraction_key:
        type: string
  validation_config:
    type: object
    properties:
      validate:
        type: boolean
      mode:
        type: string
  dedup_config:
    type: object
    properties:
      drop_duplicates:
        type: boolean
      dedup_key:
        type: string
      dedup_period:
        type: integer
  data_schema:
    type: object
    properties:
      type:
        type: string
  denorm_config:
    type: object
    properties:
      redis_db_host:
        type: string
      redis_db_port:
        type: string
      denorm_fields:
        type: array
        items:
          type: string
          properties:
            denorm_key:
              type: string
            redis_db:
              type: integer
            denorm_out_field:
              type: string
  router_config:
    type: object
    properties:
      topic:
        type: string
    required:
      - topic
  tags:
    type: array
    items:
      type: string
  status:
    type: string
    enum:
      - ACTIVE
      - DISABLED
  created_by:
    type: string
  updated_by:
    type: string
  published_date:
    type: string
```
### List
**End Point**: `/obsrv/v1/datasets/list`  
**Method**: `POST`  
**Body Schema**: 
```
description: filters are required
type: object
properties:
  filters:
    type: object
    properties:
      status:
        oneOf:
          - type: string
          - type: array
            items:
              type: string
            enum:
              - ACTIVE
              - DISABLED
```
## Data In(Ingestion)
### 
**End Point**: `/obsrv/v1/data/{datasetId}`  
**Method**: `POST`  
**Body Schema**:
```
description: datasetId in request params is required
type: object
properties:
  data:
    type: object
```
## Data Query
### Native Query
**End Point**: `/obsrv/v1/query`  
**Method**: `POST`  
**Body Schema**:
```
description: context parameter is required
type: object
properties:
  context:
    type: object
    properties:
      dataSource:
        type: string
  query:
    type: object
    properties:
      queryType:
        type: string
        enum:
          - scan
          - groupBy
          - topN
          - timeBoundary
          - search
          - timeseries
      dataSource:
        type: string
      dimensions:
        type: array
        items:
          type: string
      granularity:
        type: string
      intervals:
        oneOf:
          - type: string
          - type: array
            items:
              type: string
      filter:
        type: object
        properties:
          type:
            type: string
          dimension:
            type: string
          value:
            type: string
      aggregations:
        type: array
        items:
          properties:
            type:
              type: string
            name:
              type: string
            fieldName:
              type: string
```
### SQL Query
**End Point**: `/obsrv/v1/sql-query`  
**Method**: `POST`  
**Body Schema**:
```
description: context parameter is required
type: object
properties:
  context:
    type: object
    properties:
      dataSource:
        type: string
  querySql:
    type: object
    properties:
      query:
        type: string
```
## DataSource APIs
The following CRUL APIs are enabled for Datasources:
### Create
**End Point**: `/obsrv/v1/datasources`  
**Method**: `POST`  
**Body Schema**:
```
description: dataset_id, datasource parameters are required
type: object
properties:
  id:
    type: string
  dataset_id:
    type: string
  ingestion_spec:
    type: object
  datasource:
    type: string
  datasource_ref:
    type: string
  retention_period:
    type: object
  archival_policy:
    type: object
  purge_policy:
    type: object
  backup_config:
    type: object
  status:
    type: string
    enum:
      - ACTIVE
      - DISABLED
  created_by:
    type: string
  updated_by:
    type: string
  published_date:
    type: string
```
### Read 
**End Point**: `/obsrv/v1/datasources/{datasourceId}`  
**Method**: `GET`  
**Params**:
```
name: datasourceId
in: path
required: true
schema:
  type: string
  format: uuid
```
### Update
**End Point**: `/obsrv/v1/datasources`  
**Method**: `PATCH`  
**Body Schema**:
```
description: dataset_id, datasource parameters are required
type: object
properties:
  id:
    type: string
  dataset_id:
    type: string
  ingestion_spec:
    type: object
  datasource:
    type: string
  datasource_ref:
    type: string
  retention_period:
    type: object
  archival_policy:
    type: object
  purge_policy:
    type: object
  backup_config:
    type: object
  status:
    type: string
    enum:
      - ACTIVE
      - DISABLED
  created_by:
    type: string
  updated_by:
    type: string
  published_date:
    type: string
```
### List
**End Point**: `/obsrv/v1/datasources/list`  
**Method**: `POST`  
**Body Schema**:
```
description: filters are required
type: object
properties:
  filters:
    type: object
    properties:
      status:
        oneOf:
          - type: string
          - type: array
            items:
              type: string
            enum:
              - ACTIVE
              - DISABLED
```
## Dataset Config APIs
The following CRUL APIs are enabled to interact with Dataset Source Configurations:
### Create
**End Point**: `/obsrv/v1/datasets/source/config`  
**Method**: `POST`  
**Body Schema**:
```
description: dataset_id, connector_type are required
type: object
properties:
  id:
    type: string
  dataset_id:
    type: string
  connector_type:
    type: string
  connector_config:
    type: object
  status:
    type: string
  connector_stats:
    type: object
  created_by:
    type: string
  updated_by:
    type: string
  published_date:
    type: string

```
### Read 
**End Point**: `/obsrv/v1/datasets/source/config`  
**Method**: `GET`  
**Params**:
```
name: datasetId
in: path
required: true
schema:
  type: string
  format: uuid
```
### Update
**End Point**: `/obsrv/v1/datasets/source/config`  
**Method**: `PATCH`  
**Body Schema**:
```
description: dataset_id, connector_type are required
type: object
properties:
  id:
    type: string
  dataset_id:
    type: string
  connector_type:
    type: string
  connector_config:
    type: object
  status:
    type: string
  connector_stats:
    type: object
  created_by:
    type: string
  updated_by:
    type: string
  published_date:
    type: string
```
### List
**End Point**: `/obsrv/v1/datasets/source/config/list`  
**Method**: `POST`  
**Body Schema**:
```
description: filters are required
type: object
properties:
  filters:
    type: object
    properties:
      status:
        oneOf:
          - type: string
          - type: array
            items:
              type: string
            enum:
              - ACTIVE
              - DISABLED
```