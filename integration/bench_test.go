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

// BenchmarkBackfill1000Rows measures end-to-end backfill throughput for a
// 1000-row table with chunk size 100.
func BenchmarkBackfill1000Rows(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	if err := itest.ResetTables(ctx); err != nil {
		b.Fatalf("reset tables: %v", err)
	}

	src, err := itest.SourceConn(ctx)
	if err != nil {
		b.Fatalf("source conn: %v", err)
	}
	const rowCount = 1000
	for i := int64(1); i <= rowCount; i++ {
		if _, err := src.Exec(ctx,
			`INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)`,
			i, fmt.Sprintf("owner-%d", i), i*100); err != nil {
			b.Fatalf("insert: %v", err)
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
		b.Fatalf("start capture reader: %v", err)
	}
	defer reader.Close()

	recCtx, recCancel := context.WithCancel(ctx)
	capErr := make(chan error, 1)
	go func() {
		if err := reader.Run(recCtx); err != nil && !errors.Is(err, context.Canceled) {
			capErr <- err
		}
		close(capErr)
	}()
	defer func() {
		_ = reader.Close()
		select {
		case err := <-capErr:
			if err != nil {
				b.Errorf("capture reader error: %v", err)
			}
		case <-time.After(5 * time.Second):
			b.Error("capture reader did not stop")
		}
	}()

	jobCfg := model.JobConfig{
		JobID:             "bench-backfill",
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
		b.Fatalf("ensure tables: %v", err)
	}
	upperBound, err := scan.NewChunkReader(itest.SourceDSN()).UpperBound(ctx)
	if err != nil {
		b.Fatalf("upper bound: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cp, err := cpStore.CreateJob(ctx, jobCfg, upperBound)
		if err != nil {
			b.Fatalf("create job: %v", err)
		}

		consumer, err := kafka.NewConsumer(itest.KafkaBrokers(), itest.KafkaTopic(), cp.NextKafkaOffset, func(data []byte) (model.Change, error) {
			var codec capture.JSONCodec
			return codec.Decode(data)
		})
		if err != nil {
			b.Fatalf("create consumer: %v", err)
		}

		rec := reconcile.New(reconcile.Config{
			JobConfig:       jobCfg,
			Checkpoint:      cp,
			Consumer:        consumer,
			CheckpointStore: cpStore,
			MarkerStore:     marker.NewStore(itest.SourceDSN()),
			Scanner:         scan.NewChunkReader(itest.SourceDSN()),
			Sink:            sink.NewMutator(),
		})

		done := make(chan error, 1)
		go func() {
			done <- rec.Run(recCtx)
		}()
		b.StartTimer()

		dst, err := itest.DestConn(ctx)
		if err != nil {
			b.Fatalf("dest conn: %v", err)
		}
		var completed int64
		for j := 0; j < 300; j++ {
			if err := dst.QueryRow(ctx, `SELECT completed_through_id FROM seam_checkpoints WHERE job_id = $1`, jobCfg.JobID).Scan(&completed); err != nil {
				b.Fatalf("read checkpoint: %v", err)
			}
			if completed >= upperBound {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		dst.Close(context.Background())
		if completed < upperBound {
			b.Fatalf("backfill did not complete")
		}
		consumer.Close()
	}
	recCancel()
}
