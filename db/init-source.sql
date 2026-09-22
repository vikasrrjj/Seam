CREATE TABLE IF NOT EXISTS accounts (
    id BIGINT PRIMARY KEY,
    owner TEXT NOT NULL,
    balance_cents BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS seam_marker (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    attempt TEXT NOT NULL,
    kind TEXT NOT NULL,
    chunk_min_id BIGINT NOT NULL,
    chunk_max_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Publication must be created after tables exist. Replication slot is created
-- by the capture process.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'seam_pub'
    ) THEN
        CREATE PUBLICATION seam_pub FOR TABLE accounts, seam_marker;
    END IF;
END
$$;
