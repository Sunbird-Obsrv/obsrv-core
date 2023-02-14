CREATE TABLE IF NOT EXISTS system_config (
    key text PRIMARY KEY,
    value text NOT NULL,
    value_type text NOT NULL,
    label text NOT NULL,
    description text NOT NULL,
    category text NOT NULL,
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