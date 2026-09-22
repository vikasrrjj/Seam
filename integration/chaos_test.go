//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/reconcile"
	"example.com/seam/internal/recovery"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
	"example.com/seam/integration/itest"
)

// TestChaos_CrashRestart verifies source == destination after concurrent random
// mutations and repeated reconciler crashes/restarts.
func TestChaos_CrashRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	if err := itest.ResetTables(ctx); err != nil {
		t.Fatalf("reset tables: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	// Seed with a small table so there is data to mutate.
	for i := int64(1); i <= 20; i++ {
		if _, err := src.Exec(ctx,
			`INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)`,
			i, fmt.Sprintf("owner-%d", i), i*100); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}
	src.Close(context.Background())

	readerCfg := capture.ReaderConfig{
		SQLDSN:         itest.SourceDSN(),
		ReplicationDSN: itest.SourceReplDSN(),
		Slot:           "seam_itest_slot",
		Publication:    "seam_pub",
		KafkaBrokers:   itest.KafkaBrokers(),
		KafkaTopic:     itest.KafkaTopic(),
		Generation:     "gen:0",
	}
	reader, err := capture.StartReader(ctx, readerCfg)
	if err != nil {
		t.Fatalf("start capture reader: %v", err)
	}
	defer reader.Close()

	capErr := make(chan error, 1)
	go func() {
		if err := reader.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			capErr <- err
		}
		close(capErr)
	}()
	defer func() {
		_ = reader.Close()
		select {
		case err := <-capErr:
			if err != nil {
				t.Errorf("capture reader error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("capture reader did not stop")
		}
	}()

	jobCfg := model.JobConfig{
		JobID:             "chaos",
		SourceDSN:         itest.SourceDSN(),
		SourceReplDSN:     itest.SourceReplDSN(),
		SourceSlot:        "seam_itest_slot",
		SourcePublication: "seam_pub",
		DestDSN:           itest.DestDSN(),
		KafkaBrokers:      itest.KafkaBrokers(),
		KafkaTopic:        itest.KafkaTopic(),
		ChunkSize:         7,
	}
	cpStore := checkpoint.NewStore(itest.DestDSN())
	if err := cpStore.EnsureTables(ctx); err != nil {
		t.Fatalf("ensure tables: %v", err)
	}
	upperBound, err := scan.NewChunkReader(itest.SourceDSN()).UpperBound(ctx)
	if err != nil {
		t.Fatalf("upper bound: %v", err)
	}
	if _, err := cpStore.CreateJob(ctx, jobCfg, upperBound); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// startReconciler starts the reconciler from the given checkpoint and returns
	// a cancel func + error channel.
	startReconciler := func(ctx context.Context, checkpoint *model.Checkpoint) (context.CancelFunc, <-chan error) {
		recCtx, recCancel := context.WithCancel(ctx)
		consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), checkpoint.NextKafkaOffset, func(data []byte) (model.Change, error) {
			var codec capture.JSONCodec
			return codec.Decode(data)
		})
		if err != nil {
			t.Fatalf("create consumer at offset %d: %v", checkpoint.NextKafkaOffset, err)
		}
		rec := reconcile.New(reconcile.Config{
			JobConfig:       jobCfg,
			Checkpoint:      checkpoint,
			Consumer:        consumer,
			CheckpointStore: cpStore,
			MarkerStore:     marker.NewStore(itest.SourceDSN()),
			Scanner:         scan.NewChunkReader(itest.SourceDSN()),
			Sink:            sink.NewMutator(),
		})
		recErr := make(chan error, 1)
		go func() {
			if err := rec.Run(recCtx); err != nil && !errors.Is(err, context.Canceled) {
				recErr <- err
			}
			close(recErr)
			consumer.Close()
		}()
		return recCancel, recErr
	}

	// Reconciler restart loop. A separate signal channel lets the chaos goroutine
	// crash only the current reconciler instance without stopping the loop.
	restartCh := make(chan struct{})
	recErrCh := make(chan error, 1)
	loopCtx, loopCancel := context.WithCancel(ctx)
	go func() {
		for {
			// Mirror production restart behavior: verify continuity and bump the
			// attempt when a previous chunk was left unfinished.
			result, err := recovery.Recover(loopCtx, cpStore, jobCfg.JobID, jobCfg.SourceDSN)
			if err != nil {
				recErrCh <- fmt.Errorf("recover: %w", err)
				return
			}
			latest := result.Checkpoint
			if latest == nil {
				recErrCh <- fmt.Errorf("checkpoint missing")
				return
			}
			recCtx, recCancel := context.WithCancel(loopCtx)
			cancel, ch := startReconciler(recCtx, latest)
			select {
			case err := <-ch:
				_ = cancel
				if err != nil && !errors.Is(err, context.Canceled) {
					recErrCh <- err
					return
				}
				if loopCtx.Err() != nil {
					recErrCh <- nil
					return
				}
				// Exited after cancel: restart.
			case <-restartCh:
				recCancel()
				if err := <-ch; err != nil && !errors.Is(err, context.Canceled) {
					recErrCh <- err
					return
				}
				_ = cancel
				// Shutdown drained; restart immediately.
			case <-loopCtx.Done():
				recCancel()
				<-ch
				_ = cancel
				recErrCh <- nil
				return
			}
		}
	}()

	// Chaos: random mutations and occasional reconciler crashes.
	mutationCtx, stopMutations := context.WithCancel(ctx)
	var maxID int64 = 20
	mutErr := make(chan error, 1)
	go func() {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		src, err := itest.SourceConn(ctx)
		if err != nil {
			mutErr <- err
			return
		}
		defer src.Close(context.Background())

		nextCrash := time.After(4 * time.Second)
		crashCount := 0
		mutations := 0
		for mutationCtx.Err() == nil {
			select {
			case <-nextCrash:
				if crashCount < 3 {
					crashCount++
					close(restartCh)
					restartCh = make(chan struct{})
					nextCrash = time.After(4 * time.Second)
					continue
				}
			default:
			}

			op := rng.Intn(4)
			switch op {
			case 0: // insert
				maxID++
				if _, err := src.Exec(ctx,
					`INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)`,
					maxID, fmt.Sprintf("chaos-%d", maxID), rng.Int63n(10000)); err != nil {
					mutErr <- fmt.Errorf("insert %d: %w", maxID, err)
					return
				}
			case 1: // update existing
				id := rng.Int63n(maxID) + 1
				if _, err := src.Exec(ctx,
					`UPDATE accounts SET balance_cents = $1 WHERE id = $2`,
					rng.Int63n(10000), id); err != nil {
					mutErr <- fmt.Errorf("update %d: %w", id, err)
					return
				}
			case 2: // delete
				id := rng.Int63n(maxID) + 1
				if _, err := src.Exec(ctx, `DELETE FROM accounts WHERE id = $1`, id); err != nil {
					mutErr <- fmt.Errorf("delete %d: %w", id, err)
					return
				}
			case 3: // update owner
				id := rng.Int63n(maxID) + 1
				if _, err := src.Exec(ctx,
					`UPDATE accounts SET owner = $1 WHERE id = $2`,
					fmt.Sprintf("mutated-%d-%d", id, mutations), id); err != nil {
					mutErr <- fmt.Errorf("update owner %d: %w", id, err)
					return
				}
			}
			mutations++
			if mutations%50 == 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
		mutErr <- nil
	}()

	// Let chaos run for a bounded time.
	select {
	case <-time.After(15 * time.Second):
	case err := <-mutErr:
		if err != nil {
			t.Fatalf("mutation error: %v", err)
		}
	}
	stopMutations()

	// Wait for mutation goroutine to finish.
	if err := <-mutErr; err != nil {
		t.Fatalf("mutation error: %v", err)
	}

	// Wait for reconciler to catch up to final source state.
	srcCheck, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source check conn: %v", err)
	}
	defer srcCheck.Close(context.Background())

	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	defer dst.Close(context.Background())

	var sourceCount, destCount int
	converged := false
	for i := 0; i < 120 && !converged; i++ {
		if err := srcCheck.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&sourceCount); err != nil {
			t.Fatalf("count source: %v", err)
		}
		if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&destCount); err != nil {
			t.Fatalf("count dest: %v", err)
		}
		if sourceCount != destCount {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		// Counts match: verify the full contents match exactly. The scan cursor
		// may legitimately stop below the initial upper bound when upper-bound
		// rows were deleted during the chaos window.
		if err := verifyExact(ctx); err == nil {
			converged = true
		} else {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !converged {
		var completed, nextOffset, applied int64
		_ = dst.QueryRow(ctx, `SELECT completed_through_id, next_kafka_offset FROM seam_checkpoints WHERE job_id = $1`, jobCfg.JobID).Scan(&completed, &nextOffset)
		_ = dst.QueryRow(ctx, `SELECT COUNT(*) FROM seam_applied_txs WHERE job_id = $1`, jobCfg.JobID).Scan(&applied)
		select {
		case recErr := <-recErrCh:
			if recErr != nil {
				t.Fatalf("catch-up failed (reconciler error %q): source=%d dest=%d completed_through=%d next_offset=%d applied_txs=%d",
					recErr, sourceCount, destCount, completed, nextOffset, applied)
			}
		default:
		}
		t.Fatalf("catch-up failed: source=%d dest=%d completed_through=%d next_offset=%d applied_txs=%d",
			sourceCount, destCount, completed, nextOffset, applied)
	}

	// Stop reconciler loop gracefully.
	loopCancel()
	select {
	case err := <-recErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("reconciler error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reconciler loop did not stop")
	}

	// Final exact verification: every source row matches destination, and counts match.
	if err := verifyExact(ctx); err != nil {
		t.Fatalf("final verification failed: %v", err)
	}

	t.Logf("chaos test passed: source rows=%d dest rows=%d", sourceCount, destCount)
}

func verifyExact(ctx context.Context) error {
	src, err := itest.SourceConn(ctx)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := itest.DestConn(ctx)
	if err != nil {
		return fmt.Errorf("connect dest: %w", err)
	}
	defer dst.Close(context.Background())

	var srcCount, dstCount int
	if err := src.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&srcCount); err != nil {
		return err
	}
	if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&dstCount); err != nil {
		return err
	}
	if srcCount != dstCount {
		return fmt.Errorf("row count mismatch: source=%d dest=%d", srcCount, dstCount)
	}

	rows, err := src.Query(ctx, `SELECT id, owner, balance_cents FROM accounts ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id, bal int64
		var owner string
		if err := rows.Scan(&id, &owner, &bal); err != nil {
			return err
		}
		var dstOwner string
		var dstBal int64
		err := dst.QueryRow(ctx, `SELECT owner, balance_cents FROM accounts WHERE id = $1`, id).Scan(&dstOwner, &dstBal)
		if err != nil {
			return fmt.Errorf("dest missing row %d: %w", id, err)
		}
		if dstOwner != owner || dstBal != bal {
			return fmt.Errorf("row %d mismatch: source=(%s,%d) dest=(%s,%d)", id, owner, bal, dstOwner, dstBal)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Source count matched dest count and every source row matched, so the tables
	// are identical.
	return nil
}
