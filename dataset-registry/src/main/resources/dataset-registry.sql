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

CREATE TABLE IF NOT EXISTS cloud_backup_config (
    id text PRIMARY KEY,
    dataset_id text NULL REFERENCES datasets (id),
    consumer_group text NOT NULL,
    service_name text NOT NULL,
    base_path text NOT NULL,
    timestamp_key text NOT NULL,
    fallback_timestamp_key text NOT NULL,
    topic text NOT NULL,
    max_file_size INT,
    max_file_age INT,
    partition_prefix_enabled boolean,
    partition_prefix_key text,
    partition_prefix_mapping text,
    output_file_pattern text NOT NULL,
    message_parser text NOT NULL,
    monitoring_config json,
    status text NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_date Date NOT NULL,
    updated_date Date NOT NULL
);

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