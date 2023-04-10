CREATE TABLE IF NOT EXISTS datasets (
    id text PRIMARY KEY,
    type text NOT NULL,
    validation_config json,
    extraction_config json,
    dedup_config json,
    data_schema json,
    denorm_config json,
    router_config json NOT NULL,
    dataset_config json NOT NULL,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date timestamp NOT NULL,
    updated_date timestamp NOT NULL
);

CREATE INDEX IF NOT EXISTS datasets_status ON datasets(status);

CREATE TABLE IF NOT EXISTS datasources (
    datasource text PRIMARY KEY,
    dataset_id text REFERENCES datasets (id),
    ingestion_spec json NOT NULL,
    datasource_ref text NOT NULL,
    retention_period json,
    archival_policy json,
    purge_policy json,
    backup_config json NOT NULL,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL
);
CREATE INDEX IF NOT EXISTS datasources_status ON datasources(status);
CREATE INDEX IF NOT EXISTS datasources_dataset ON datasources(dataset_id);

CREATE TABLE IF NOT EXISTS dataset_transformations (
    id text PRIMARY KEY,
    dataset_id text REFERENCES datasets (id),
    field_key text NOT NULL,
    transformation_function text NOT NULL,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL,
    UNIQUE(field_key, dataset_id)
);
CREATE INDEX IF NOT EXISTS dataset_transformations_status ON dataset_transformations(status);
CREATE INDEX IF NOT EXISTS dataset_transformations_dataset ON dataset_transformations(dataset_id);

CREATE TABLE IF NOT EXISTS dataset_source_config (
    id SERIAL PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES datasets (id),
    connector_type text NOT NULL,
    connector_config json NOT NULL,
    connector_stats json NOT NULL,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL,
    UNIQUE(dataset_id)
);
CREATE INDEX IF NOT EXISTS dataset_source_config_status ON dataset_source_config(status);
CREATE INDEX IF NOT EXISTS dataset_source_config_dataset ON dataset_source_config(dataset_id);