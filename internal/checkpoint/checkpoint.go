// Package checkpoint persists Seam's durable progress state in destination PostgreSQL.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"

	"example.com/seam/internal/model"
	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

// Store owns the destination checkpoint tables.
type Store struct {
	dsn string
}

func NewStore(dsn string) *Store {
	return &Store{dsn: dsn}
}

// Conn opens a connection to the destination database.
func (s *Store) Conn(ctx context.Context) (*pgx.Conn, error) {
	return transport.ConnectPostgres(ctx, s.dsn)
}

func (s *Store) conn(ctx context.Context) (*pgx.Conn, error) {
	return s.Conn(ctx)
}

// EnsureTables creates the checkpoint schema if it does not exist.
func (s *Store) EnsureTables(ctx context.Context) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(ctx, `
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
	`)
	return err
}

// Fingerprint returns a deterministic validation token for the destination
// accounts table schema. Empty string means any schema is accepted.
func Fingerprint(ctx context.Context, conn *pgx.Conn) (string, error) {
	rows, err := conn.Query(ctx, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'accounts'
		ORDER BY ordinal_position`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var cols []map[string]string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return "", err
		}
		cols = append(cols, map[string]string{"name": name, "type": typ})
	}
	b, _ := json.Marshal(cols)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])[:16], nil
}

// CreateJob inserts a new job and checkpoint. It returns the checkpoint.
func (s *Store) CreateJob(ctx context.Context, cfg model.JobConfig, upperBound int64) (*model.Checkpoint, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	fp, err := Fingerprint(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("fingerprint destination schema: %w", err)
	}

	generation := "gen:0"
	attempt := "gen:0:attempt:0"
	checkpoint := &model.Checkpoint{
		JobID:            cfg.JobID,
		Generation:       generation,
		Attempt:          attempt,
		ScanUpperBound:   upperBound,
		CompletedThrough: math.MinInt64,
		NextKafkaOffset:  0,
		Active:           true,
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO seam_jobs (job_id, source_dsn, source_slot, source_publication, table_schema, table_name, kafka_topic, generation, scan_upper_bound, schema_fingerprint)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		cfg.JobID, cfg.SourceDSN, cfg.SourceSlot, cfg.SourcePublication, "public", "accounts", cfg.KafkaTopic, generation, upperBound, fp); err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO seam_checkpoints (job_id, generation, attempt, scan_upper_bound, completed_through_id, next_kafka_offset, active)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		cfg.JobID, generation, attempt, upperBound, checkpoint.CompletedThrough, checkpoint.NextKafkaOffset, checkpoint.Active); err != nil {
		return nil, fmt.Errorf("insert checkpoint: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return checkpoint, nil
}

// LoadCheckpoint returns the current checkpoint for a job, or nil if not found.
func (s *Store) LoadCheckpoint(ctx context.Context, jobID string) (*model.Checkpoint, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	var cp model.Checkpoint
	var lastLSN *string
	err = conn.QueryRow(ctx, `
		SELECT job_id, generation, attempt, scan_upper_bound, completed_through_id, next_kafka_offset, last_applied_lsn, active
		FROM seam_checkpoints WHERE job_id = $1`, jobID).Scan(
		&cp.JobID, &cp.Generation, &cp.Attempt, &cp.ScanUpperBound, &cp.CompletedThrough, &cp.NextKafkaOffset, &lastLSN, &cp.Active,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if lastLSN != nil {
		cp.LastAppliedLSN = *lastLSN
	}
	return &cp, nil
}

// JobRecord is the persisted job metadata.
type JobRecord struct {
	Config           model.JobConfig
	Generation       string
	ScanUpperBound   int64
	SchemaFingerprint string
}

// LoadJobRecord returns the persisted configuration and generation for a job.
func (s *Store) LoadJobRecord(ctx context.Context, jobID string) (*JobRecord, error) {
	conn, err := s.conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	var rec JobRecord
	var schema, table string
	if err := conn.QueryRow(ctx, `
		SELECT job_id, source_dsn, source_slot, source_publication, table_schema, table_name, kafka_topic, generation, scan_upper_bound, schema_fingerprint
		FROM seam_jobs WHERE job_id = $1`, jobID).Scan(
		&rec.Config.JobID, &rec.Config.SourceDSN, &rec.Config.SourceSlot, &rec.Config.SourcePublication, &schema, &table, &rec.Config.KafkaTopic, &rec.Generation, &rec.ScanUpperBound, &rec.SchemaFingerprint,
	); err != nil {
		return nil, err
	}
	return &rec, nil
}

// IsApplied returns true if the source transaction has already been applied.
func (s *Store) IsApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM seam_applied_txs WHERE job_id = $1 AND generation = $2 AND source_lsn = $3)`,
		jobID, generation, lsn).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// MarkApplied records a source transaction as applied.
func (s *Store) MarkApplied(ctx context.Context, tx pgx.Tx, jobID, generation, lsn string, xid uint32) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO seam_applied_txs (job_id, generation, source_lsn, source_xid) VALUES ($1, $2, $3, $4)
		ON CONFLICT (job_id, generation, source_lsn) DO NOTHING`,
		jobID, generation, lsn, fmt.Sprintf("%d", xid))
	return err
}

// UpdateCheckpoint writes the checkpoint inside the caller's transaction.
func (s *Store) UpdateCheckpoint(ctx context.Context, tx pgx.Tx, cp *model.Checkpoint) error {
	_, err := tx.Exec(ctx, `
		UPDATE seam_checkpoints SET
			generation = $2,
			attempt = $3,
			scan_upper_bound = $4,
			completed_through_id = $5,
			next_kafka_offset = $6,
			last_applied_lsn = $7,
			active = $8,
			updated_at = NOW()
		WHERE job_id = $1`,
		cp.JobID, cp.Generation, cp.Attempt, cp.ScanUpperBound, cp.CompletedThrough, cp.NextKafkaOffset, cp.LastAppliedLSN, cp.Active)
	return err
}

// EnsureDestinationTable creates the destination accounts table if needed.
func (s *Store) EnsureDestinationTable(ctx context.Context) error {
	conn, err := s.conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS accounts (
			id BIGINT PRIMARY KEY,
			owner TEXT NOT NULL,
			balance_cents BIGINT NOT NULL
		)`)
	return err
}
