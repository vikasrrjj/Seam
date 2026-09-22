// Command seam-lab is an independent verifier and failure lab for Seam.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"example.com/seam/internal/transport"
	"github.com/jackc/pgx/v5"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sourceDSN := envOrDefault("SOURCE_SQL_DSN", "postgres://postgres:postgres@localhost:5433/source?sslmode=disable")
	destDSN := envOrDefault("DEST_SQL_DSN", "postgres://postgres:postgres@localhost:5434/dest?sslmode=disable")

	switch os.Args[1] {
	case "verify":
		if err := verify(ctx, sourceDSN, destDSN); err != nil {
			fmt.Fprintf(os.Stderr, "verify failed: %v\n", err)
			os.Exit(1)
		}
	case "stale-update":
		if err := staleUpdate(ctx, sourceDSN, destDSN); err != nil {
			fmt.Fprintf(os.Stderr, "stale-update failed: %v\n", err)
			os.Exit(1)
		}
	case "checkpoint":
		jobID := envOrDefault("SEAM_JOB_ID", "seam-default")
		if err := showCheckpoint(ctx, destDSN, jobID); err != nil {
			fmt.Fprintf(os.Stderr, "checkpoint failed: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`seam-lab: independent verifier and failure lab

Commands:
  verify         Compare row counts and balances between source and destination.
  stale-update   Demonstrate a stale overwrite by copying source rows directly to
                 destination after modifying the source.
  checkpoint     Print durable checkpoint state for SEAM_JOB_ID.

Environment:
  SOURCE_SQL_DSN  Source PostgreSQL DSN (default postgres://postgres:postgres@localhost:5433/source?sslmode=disable)
  DEST_SQL_DSN    Destination PostgreSQL DSN (default postgres://postgres:postgres@localhost:5434/dest?sslmode=disable)
  SEAM_JOB_ID     Job id used by the checkpoint command (default seam-default)`)
}

func verify(ctx context.Context, sourceDSN, destDSN string) error {
	src, err := transport.ConnectPostgres(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := transport.ConnectPostgres(ctx, destDSN)
	if err != nil {
		return fmt.Errorf("connect dest: %w", err)
	}
	defer dst.Close(context.Background())

	var srcCount, dstCount int
	var srcSum, dstSum int64
	if err := src.QueryRow(ctx, `SELECT COUNT(*), COALESCE(SUM(balance_cents),0) FROM accounts`).Scan(&srcCount, &srcSum); err != nil {
		return fmt.Errorf("source aggregate: %w", err)
	}
	if err := dst.QueryRow(ctx, `SELECT COUNT(*), COALESCE(SUM(balance_cents),0) FROM accounts`).Scan(&dstCount, &dstSum); err != nil {
		return fmt.Errorf("dest aggregate: %w", err)
	}

	fmt.Printf("source rows: %d  sum: %d\ndest rows:   %d  sum: %d\n", srcCount, srcSum, dstCount, dstSum)
	if srcCount != dstCount {
		return fmt.Errorf("row count mismatch: source=%d dest=%d", srcCount, dstCount)
	}
	if srcSum != dstSum {
		return fmt.Errorf("balance sum mismatch: source=%d dest=%d", srcSum, dstSum)
	}

	// Compare every row exactly. Streaming iteration avoids loading the entire
	// table into memory.
	rows, err := src.Query(ctx, `SELECT id, owner, balance_cents FROM accounts ORDER BY id`)
	if err != nil {
		return fmt.Errorf("query source rows: %w", err)
	}
	defer rows.Close()

	var checked int
	var mismatches int
	for rows.Next() {
		var id, bal int64
		var owner string
		if err := rows.Scan(&id, &owner, &bal); err != nil {
			return fmt.Errorf("scan source row: %w", err)
		}
		var dstID *int64
		var dstOwner string
		var dstBal int64
		if err := dst.QueryRow(ctx, `SELECT id, owner, balance_cents FROM accounts WHERE id = $1`, id).Scan(&dstID, &dstOwner, &dstBal); err != nil {
			return fmt.Errorf("dest row %d: %w", id, err)
		}
		if dstID == nil || *dstID != id || dstOwner != owner || dstBal != bal {
			mismatches++
			fmt.Printf("mismatch id=%d: src=(%s,%d) dst=(%v,%s,%d)\n", id, owner, bal, dstID, dstOwner, dstBal)
		}
		checked++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate source rows: %w", err)
	}
	if mismatches > 0 {
		return fmt.Errorf("found %d mismatched rows out of %d checked", mismatches, checked)
	}
	fmt.Printf("verify: ok (%d rows checked)\n", checked)
	return nil
}

func staleUpdate(ctx context.Context, sourceDSN, destDSN string) error {
	src, err := transport.ConnectPostgres(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := transport.ConnectPostgres(ctx, destDSN)
	if err != nil {
		return fmt.Errorf("connect dest: %w", err)
	}
	defer dst.Close(context.Background())

	// Pick a row that exists in both tables.
	var id int64
	var owner string
	var bal int64
	if err := src.QueryRow(ctx, `SELECT id, owner, balance_cents FROM accounts ORDER BY id LIMIT 1`).Scan(&id, &owner, &bal); err != nil {
		return fmt.Errorf("select source row: %w", err)
	}

	// Update source.
	newBal := bal + 1
	if _, err := src.Exec(ctx, `UPDATE accounts SET balance_cents = $1 WHERE id = $2`, newBal, id); err != nil {
		return fmt.Errorf("update source: %w", err)
	}

	// Copy the now-stale source row directly to destination (simulates a naive
	// snapshot write that races with CDC).
	if _, err := dst.Exec(ctx,
		`INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance_cents = EXCLUDED.balance_cents`,
		id, owner, newBal); err != nil {
		return fmt.Errorf("stale write dest: %w", err)
	}

	fmt.Printf("stale-update: overwrote destination id=%d with balance=%d\n", id, newBal)
	fmt.Println("Run 'seam-lab verify' after CDC catches up to see if the stale value persists.")
	return nil
}

func showCheckpoint(ctx context.Context, destDSN, jobID string) error {
	dst, err := transport.ConnectPostgres(ctx, destDSN)
	if err != nil {
		return fmt.Errorf("connect dest: %w", err)
	}
	defer dst.Close(context.Background())

	var generation, attempt string
	var upperBound, completedThrough, nextOffset int64
	var lastLSN *string
	var active bool
	err = dst.QueryRow(ctx, `
		SELECT generation, attempt, scan_upper_bound, completed_through_id, next_kafka_offset, last_applied_lsn, active
		FROM seam_checkpoints WHERE job_id = $1`, jobID).Scan(
		&generation, &attempt, &upperBound, &completedThrough, &nextOffset, &lastLSN, &active)
	if err == pgx.ErrNoRows {
		fmt.Printf("no checkpoint found for job %q\n", jobID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("query checkpoint: %w", err)
	}

	fmt.Printf("job:             %s\n", jobID)
	fmt.Printf("generation:      %s\n", generation)
	fmt.Printf("attempt:         %s\n", attempt)
	fmt.Printf("scan_upper_bound: %d\n", upperBound)
	fmt.Printf("completed_through: %d\n", completedThrough)
	fmt.Printf("next_offset:     %d\n", nextOffset)
	if lastLSN != nil {
		fmt.Printf("last_applied_lsn: %s\n", *lastLSN)
	} else {
		fmt.Println("last_applied_lsn: <none>")
	}
	fmt.Printf("active:          %v\n", active)
	return nil
}

func envOrDefault(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}
