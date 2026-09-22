//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/reconcile"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
	"example.com/seam/integration/itest"
)

// TestPhase0_CDCFlow proves source changes reach the destination via Kafka.
func TestPhase0_CDCFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := itest.ResetTables(ctx); err != nil {
		t.Fatalf("reset tables: %v", err)
	}

	// Start capture reader.
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

	recCtx, recCancel := context.WithCancel(ctx)
	defer recCancel()

	capErr := make(chan error, 1)
	go func() {
		if err := reader.Run(recCtx); err != nil && !errors.Is(err, context.Canceled) {
			capErr <- err
		}
		close(capErr)
	}()

	// Start seam reconciler.
	jobCfg := model.JobConfig{
		JobID:             "phase0",
		SourceDSN:         itest.SourceDSN(),
		SourceReplDSN:     itest.SourceReplDSN(),
		SourceSlot:        "seam_itest_slot",
		SourcePublication: "seam_pub",
		DestDSN:           itest.DestDSN(),
		KafkaBrokers:      itest.KafkaBrokers(),
		KafkaTopic:        itest.KafkaTopic(),
		ChunkSize:         100,
	}
	cpStore := checkpoint.NewStore(itest.DestDSN())
	if err := cpStore.EnsureTables(ctx); err != nil {
		t.Fatalf("ensure tables: %v", err)
	}
	upperBound, err := scan.NewChunkReader(itest.SourceDSN()).UpperBound(ctx)
	if err != nil {
		t.Fatalf("upper bound: %v", err)
	}
	cp, err := cpStore.CreateJob(ctx, jobCfg, upperBound)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), cp.NextKafkaOffset, func(data []byte) (model.Change, error) {
		var codec capture.JSONCodec
		return codec.Decode(data)
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	rec := reconcile.New(reconcile.Config{
		JobConfig:       jobCfg,
		Checkpoint:      cp,
		Consumer:        consumer,
		CheckpointStore: cpStore,
		MarkerStore:     marker.NewStore(itest.SourceDSN()),
		Scanner:         scan.NewChunkReader(itest.SourceDSN()),
		Sink:            sink.NewMutator(),
	})

	recErr := make(chan error, 1)
	go func() {
		recErr <- rec.Run(recCtx)
	}()

	// Give seam time to finish any initial chunk.
	time.Sleep(3 * time.Second)

	// Apply source mutations.
	src, err := itest.SourceConn(ctx)
	if err != nil {
		t.Fatalf("source conn: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO accounts (id, owner, balance_cents) VALUES (1, 'vik', 100)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := src.Exec(ctx, `UPDATE accounts SET owner = 'vikas' WHERE id = 1`); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := src.Exec(ctx, `DELETE FROM accounts WHERE id = 1`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	src.Close(context.Background())

	// Verify destination eventually reflects the delete.
	dst, err := itest.DestConn(ctx)
	if err != nil {
		t.Fatalf("dest conn: %v", err)
	}
	defer dst.Close(context.Background())

	var rows int
	for i := 0; i < 40; i++ {
		if err := dst.QueryRow(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&rows); err != nil {
			t.Fatalf("count dest: %v", err)
		}
		if rows == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if rows != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", rows)
	}

	recCancel()
	select {
	case err := <-recErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("reconciler error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not stop")
	}

	// Force the capture connection closed so the reader goroutine exits.
	_ = reader.Close()
	select {
	case err := <-capErr:
		if err != nil {
			t.Fatalf("capture reader error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("capture reader did not stop")
	}

	fmt.Println("Phase 0 CDC flow verified")
}
