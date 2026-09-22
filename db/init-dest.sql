CREATE TABLE IF NOT EXISTS accounts (
    id BIGINT PRIMARY KEY,
    owner TEXT NOT NULL,
    balance_cents BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS seam_jobs (
    job_id TEXT PRIMARY KEY,
    source_dsn TEXT NOT NULL,
    source_slot TEXT NOT NULL,
    source_publication TEXT NOT NULL,
    table_schema TEXT NOT NULL,
    table_name TEXT NOT NULL,
    kafka_topic TEXT NOT NULL,
    generation TEXT NOT NULL,
    scan_upper_bound BIGINT NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seam_checkpoints (
    job_id TEXT PRIMARY KEY REFERENCES seam_jobs(job_id),
    generation TEXT NOT NULL,
    attempt TEXT NOT NULL,
    scan_upper_bound BIGINT NOT NULL,
    completed_through_id BIGINT NOT NULL DEFAULT -1,
    next_kafka_offset BIGINT NOT NULL DEFAULT 0,
    last_applied_lsn TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seam_applied_txs (
    job_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    source_lsn TEXT NOT NULL,
    source_xid TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, generation, source_lsn)
);
