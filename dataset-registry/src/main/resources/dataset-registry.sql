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

CREATE TABLE IF NOT EXISTS datasources (
    id text PRIMARY KEY,
    dataset_id text REFERENCES datasets (id),
    ingestion_spec json NOT NULL,
    datasource text NOT NULL,
    retention_period json,
    archival_policy json,
    purge_policy json,
    backup_config json NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_transformations (
    id text PRIMARY KEY,
    dataset_id text REFERENCES datasets (id),
    field_key text NOT NULL,
    transformation_function text NOT NULL,
    field_out_key text NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL
);