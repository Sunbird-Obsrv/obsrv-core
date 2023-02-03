CREATE TABLE IF NOT EXISTS datasets (
    id text PRIMARY KEY,
    validation_config json,
    extraction_config json,
    dedup_config json,
    json_schema json,
    denorm_config json,
    router_config json NOT NULL,
    created_date timestamp NOT NULL,
    updated_date timestamp NOT NULL
);

CREATE TABLE dataset_ingestion_specs (
    id text PRIMARY KEY,
    dataset_id text REFERENCES datasets (id),
    ingestion_spec json NOT NULL,
    datasource text NOT NULL,
    retention_config json,
    created_date Date NOT NULL,
    updated_date Date NOT NULL
);