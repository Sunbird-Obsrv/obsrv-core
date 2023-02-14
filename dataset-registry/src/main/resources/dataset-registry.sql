CREATE TABLE IF NOT EXISTS datasets (
    id text PRIMARY KEY,
    validation_config json,
    extraction_config json,
    dedup_config json,
    data_schema json,
    denorm_config json,
    router_config json NOT NULL,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date timestamp NOT NULL,
    updated_date timestamp NOT NULL
);

CREATE INDEX IF NOT EXISTS datasets_status ON datasets(status);

CREATE TABLE IF NOT EXISTS datasources (
    id text PRIMARY KEY,
    dataset_id text REFERENCES datasets (id),
    ingestion_spec json NOT NULL,
    datasource text NOT NULL,
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
    field_out_key text NOT NULL,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL
);
CREATE INDEX IF NOT EXISTS dataset_transformations_status ON dataset_transformations(status);
CREATE INDEX IF NOT EXISTS dataset_transformations_dataset ON dataset_transformations(dataset_id);

CREATE TABLE IF NOT EXISTS datasets_denorm_config (
    id SERIAL PRIMARY KEY,
    field_key text NOT NULL,
    dataset_id text NOT NULL REFERENCES datasets (id),
    connector_type text NOT NULL,
    connector_config json NOT NULL,
    data_schema json NOT NULL,
    field_out_key text NOT NULL,
    exclude_fields text[],
    field_transformations json,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL,
    UNIQUE(field_key, dataset_id)
);
CREATE INDEX IF NOT EXISTS datasets_denorm_config_status ON datasets_denorm_config(status);
CREATE INDEX IF NOT EXISTS datasets_denorm_config_dataset ON datasets_denorm_config(dataset_id);

CREATE TABLE IF NOT EXISTS datasets_source_config (
    id SERIAL PRIMARY KEY,
    dataset_id text NOT NULL REFERENCES datasets (id),
    connector_type text NOT NULL,
    connector_config json NOT NULL,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL,
    UNIQUE(dataset_id)
);
CREATE INDEX IF NOT EXISTS datasets_source_config_status ON datasets_source_config(status);
CREATE INDEX IF NOT EXISTS datasets_source_config_dataset ON datasets_source_config(dataset_id);