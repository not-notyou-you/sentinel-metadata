-- database/migrations/004_add_datasets.sql

CREATE TABLE IF NOT EXISTS datasets (
    id                    SERIAL PRIMARY KEY,
    name                  VARCHAR NOT NULL,
    location              VARCHAR NOT NULL,
    date_start            DATE NOT NULL,
    date_end              DATE NOT NULL,
    required_tiers        JSONB,
    quality_settings      JSONB,
    status                VARCHAR NOT NULL DEFAULT 'QUEUED',
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    user_id               INTEGER
);

CREATE TABLE IF NOT EXISTS dataset_products (
    id                    SERIAL PRIMARY KEY,
    dataset_id            INTEGER NOT NULL REFERENCES datasets(id),
    product_id            INTEGER NOT NULL,
    processing_order      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_dataset_products_dataset_id ON dataset_products (dataset_id);

CREATE TABLE IF NOT EXISTS live_config (
    id                    SERIAL PRIMARY KEY,
    enabled               BOOLEAN NOT NULL DEFAULT FALSE,
    backfill_date_start   DATE,
    backfill_date_end     DATE,
    last_check_datetime   TIMESTAMP,
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dataset_scenes (
    id                    SERIAL PRIMARY KEY,
    dataset_id            INTEGER NOT NULL REFERENCES datasets(id),
    scene_id              INTEGER NOT NULL,
    stage_name            VARCHAR,
    status                VARCHAR,
    progress_percent      INTEGER,
    error_message         TEXT
);

CREATE INDEX IF NOT EXISTS idx_dataset_scenes_dataset_id ON dataset_scenes (dataset_id);

INSERT INTO schema_migrations (version, description)
VALUES ('004', 'Add datasets, dataset_products, live_config, and dataset_scenes tables')
ON CONFLICT (version) DO NOTHING;
