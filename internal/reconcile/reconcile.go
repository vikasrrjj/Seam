// Package reconcile implements Seam's core algorithm: bounded historical chunks
// reconciled against an ordered live CDC stream using LOW/HIGH markers.
package reconcile

import (
	"context"
	"fmt"
	"log"
	"sync"

	"example.com/seam/internal/capture"
	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/failpoint"
	"example.com/seam/internal/kafka"
	"example.com/seam/internal/marker"
	"example.com/seam/internal/model"
	"example.com/seam/internal/scan"
	"example.com/seam/internal/sink"
	"example.com/seam/internal/telemetry"
	"github.com/jackc/pgx/v5"
)

// Reconciler owns destination application during a Seam backfill.
type Reconciler struct {
	cfg        model.JobConfig
	cp         *model.Checkpoint
	consumer   *kafka.Consumer
	cpStore    *checkpoint.Store
	marker     *marker.Store
	scanner    *scan.ChunkReader
	sink       *sink.Mutator
	failpoints *failpoint.Registry
	metrics    *telemetry.Metrics
	codec           capture.JSONCodec
	disableEviction bool

	// mutable state protected by mu during a chunk
	mu          sync.Mutex
	chunk       model.ChunkRange
	candidates  map[int64]model.Account
	windowState WindowState
}

// WindowState tracks where the reconciler is relative to the current markers.
type WindowState int

const (
	// OutsideWindow means no LOW marker for the current attempt has been seen.
	OutsideWindow WindowState = iota
	// InsideWindow means LOW seen, HIGH not yet seen.
	InsideWindow
	// CompletingWindow means HIGH has been seen and the chunk is being finalized.
	CompletingWindow
)

// Config wires the reconciler together.
type Config struct {
	JobConfig       model.JobConfig
	Checkpoint      *model.Checkpoint
	Consumer        *kafka.Consumer
	CheckpointStore *checkpoint.Store
	MarkerStore     *marker.Store
	Scanner         *scan.ChunkReader
	Sink            *sink.Mutator
	Failpoints      *failpoint.Registry
	Metrics         *telemetry.Metrics
	// DisableEviction is a deliberately broken mode used by Phase 2 tests to
	// demonstrate stale overwrite and delete resurrection.
	DisableEviction bool
}

// New builds a reconciler. It does not start consuming.
func New(cfg Config) *Reconciler {
	r := &Reconciler{
		cfg:             cfg.JobConfig,
		cp:              cfg.Checkpoint,
		consumer:        cfg.Consumer,
		cpStore:         cfg.CheckpointStore,
		marker:          cfg.MarkerStore,
		scanner:         cfg.Scanner,
		sink:            cfg.Sink,
		failpoints:      cfg.Failpoints,
		metrics:         cfg.Metrics,
		disableEviction: cfg.DisableEviction,
	}
	if r.failpoints == nil {
		r.failpoints = failpoint.NewRegistry()
	}
	if r.metrics == nil {
		r.metrics = telemetry.NewMetrics()
	}
	return r
}

// Run drives the backfill and then continues applying live CDC. It returns when
// ctx is cancelled or a fatal error occurs.
func (r *Reconciler) Run(ctx context.Context) error {
	log.Printf("seam: starting job=%s generation=%s attempt=%s completed_through=%d upper_bound=%d next_offset=%d",
		r.cp.JobID, r.cp.Generation, r.cp.Attempt, r.cp.CompletedThrough, r.cp.ScanUpperBound, r.cp.NextKafkaOffset)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, ok, err := r.scanner.NextChunk(ctx, r.cp.CompletedThrough, r.cp.ScanUpperBound, r.cfg.ChunkSize)
		if err != nil {
			return fmt.Errorf("next chunk: %w", err)
		}
		if !ok {
			log.Printf("seam: backfill complete, continuing cdc-only mode")
			return r.runCDCOnly(ctx)
		}
		if err := r.runChunk(ctx, chunk); err != nil {
			return err
		}
	}
}

// runCDCOnly applies live changes after the backfill cursor has reached the
// scan upper bound.
func (r *Reconciler) runCDCOnly(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		records, err := r.consumer.Poll(ctx)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			continue
		}
		if err := r.flushBatch(ctx, records); err != nil {
			return err
		}
	}
}

// runChunk executes one reconciliation window.
func (r *Reconciler) runChunk(ctx context.Context, chunk model.ChunkRange) error {
	r.mu.Lock()
	r.chunk = chunk
	r.windowState = OutsideWindow
	r.mu.Unlock()

	log.Printf("seam: chunk %s starting", chunk)

	// 1. Write LOW marker.
	lowID, err := r.marker.WriteLow(ctx, r.cfg.JobID, r.cp.Attempt, chunk)
	if err != nil {
		return fmt.Errorf("write low marker: %w", err)
	}
	if err := r.maybeWait(ctx, failpoint.AfterLowMarkerCommitted); err != nil {
		return err
	}

	// 2. Read historical chunk into candidate map.
	rows, err := r.scanner.ReadChunk(ctx, chunk.Min, chunk.Max)
	if err != nil {
		return fmt.Errorf("read chunk %s: %w", chunk, err)
	}
	r.mu.Lock()
	r.candidates = make(map[int64]model.Account, len(rows))
	for _, account := range rows {
		r.candidates[account.ID] = account
	}
	r.mu.Unlock()
	log.Printf("seam: chunk %s read %d candidates", chunk, len(rows))

	if err := r.maybeWait(ctx, failpoint.AfterChunkReadBeforeReconciliation); err != nil {
		return err
	}

	// 3. Write HIGH marker.
	highID, err := r.marker.WriteHigh(ctx, r.cfg.JobID, r.cp.Attempt, chunk)
	if err != nil {
		return fmt.Errorf("write high marker: %w", err)
	}
	log.Printf("seam: markers low=%s high=%s", lowID, highID)

	// 4. Consume Kafka until HIGH is processed.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		records, err := r.consumer.Poll(ctx)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			continue
		}
		done, err := r.processChunkRecords(ctx, records)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// processChunkRecords applies a batch of Kafka records while inside a chunk.
// It returns true when the chunk is complete.
func (r *Reconciler) processChunkRecords(ctx context.Context, records []kafka.Record) (bool, error) {
	r.mu.Lock()
	state := r.windowState
	r.mu.Unlock()

	// Group records by source transaction.
	groups := groupByTransaction(records)

	for idx, group := range groups {
		// Determine the effective window state for this group based on markers.
		newState, err := r.evaluateMarkers(group, state)
		if err != nil {
			return false, err
		}
		state = newState

		r.mu.Lock()
		r.windowState = state
		r.mu.Unlock()

		// If the group contains the HIGH marker for the current attempt, the
		// chunk completion transaction includes surviving candidates.
		if state == CompletingWindow {
			if err := r.completeChunk(ctx, group); err != nil {
				return false, err
			}
			// Drain any remaining groups in this batch in CDC-only mode. They
			// were fetched into this buffer; dropping them would skip source
			// transactions until the next poll re-fetch never happens (the
			// consumer position has already advanced).
			for _, g := range groups[idx:] {
				if skip, err := r.skipAlreadyCompletedGroup(g); err != nil {
					return false, err
				} else if skip {
					continue
				}
				if _, err := r.evaluateMarkers(g, OutsideWindow); err != nil {
					return false, err
				}
				if err := r.applySourceTransaction(ctx, g, false); err != nil {
					return false, err
				}
			}
			return true, nil
		}

		// Otherwise apply the source transaction normally, evicting candidates
		// that are touched while inside the window.
		if err := r.applySourceTransaction(ctx, group, state == InsideWindow); err != nil {
			return false, err
		}
	}

	return false, nil
}

// skipAlreadyCompletedGroup reports whether a group is a stale marker-only
// source transaction that must not be re-evaluated after the chunk completed.
func (r *Reconciler) skipAlreadyCompletedGroup(group []kafka.Record) (bool, error) {
	for _, rec := range group {
		if rec.Change.Marker != nil &&
			rec.Change.Marker.JobID == r.cfg.JobID &&
			rec.Change.Marker.Attempt == r.cp.Attempt {
			// All markers of the current attempt for this chunk were consumed
			// by completeChunk.
			return true, nil
		}
	}
	return false, nil
}

// evaluateMarkers scans a transaction group for markers belonging to the
// current attempt and updates the window state.
func (r *Reconciler) evaluateMarkers(group []kafka.Record, state WindowState) (WindowState, error) {
	for _, rec := range group {
		if rec.Change.Marker == nil {
			continue
		}
		marker := rec.Change.Marker
		if marker.JobID != r.cfg.JobID || marker.Attempt != r.cp.Attempt {
			// Stale marker from a previous attempt; ignore.
			continue
		}
		switch marker.Kind {
		case model.MarkerLow:
			if state != OutsideWindow {
				return state, fmt.Errorf("unexpected LOW marker %s in state %d (window chunk=%s)", marker.ID, state, r.chunk)
			}
			state = InsideWindow
		case model.MarkerHigh:
			if state != InsideWindow {
				return state, fmt.Errorf("unexpected HIGH marker %s in state %d (window chunk=%s)", marker.ID, state, r.chunk)
			}
			state = CompletingWindow
		default:
			return state, fmt.Errorf("unknown marker kind %q", marker.Kind)
		}
	}
	return state, nil
}

// applySourceTransaction applies one source transaction worth of Kafka records
// to the destination. When inside the reconciliation window, any touched
// candidate key is removed from the candidate map.
func (r *Reconciler) applySourceTransaction(ctx context.Context, group []kafka.Record, insideWindow bool) error {
	if len(group) == 0 {
		return nil
	}
	source := group[0].Change.Source

	conn, err := r.cpStore.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// NOTE: A source transaction may be split across two Kafka poll batches.
	// Its continuation group shares the same commit LSN, so IsApplied alone
	// cannot gate application. Every account operation below is idempotent
	// (upsert of an exact row value / delete by id), so re-applying a group is
	// always safe. MarkApplied still records the LSN for bookkeeping.
	cdcCount := 0
	for _, rec := range group {
		if err := r.applyChange(ctx, tx, rec.Change, insideWindow); err != nil {
			return err
		}
		if rec.Change.Account != nil {
			cdcCount++
		}
	}
	if cdcCount > 0 {
		r.metrics.RecordCDC(cdcCount)
	}
	if err := r.cpStore.MarkApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN, source.XID); err != nil {
		return err
	}

	lastOffset := group[len(group)-1].Offset
	if err := r.updateCheckpoint(ctx, tx, lastOffset, source.LSN); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source transaction: %w", err)
	}
	return nil
}

// applyChange applies one change. If insideWindow and the change touches a
// candidate key, that candidate is evicted.
func (r *Reconciler) applyChange(ctx context.Context, tx pgx.Tx, change model.Change, insideWindow bool) error {
	switch {
	case change.Account != nil:
		if insideWindow && !r.disableEviction {
			r.mu.Lock()
			delete(r.candidates, change.Account.ID)
			r.mu.Unlock()
		}
		return r.sink.Apply(ctx, tx, change)
	case change.Marker != nil:
		// Markers carry no destination mutation.
		return nil
	default:
		return fmt.Errorf("change has no payload")
	}
}

// completeChunk commits the chunk completion transaction: surviving candidates,
// checkpoint cursor, and Kafka progress through HIGH.
func (r *Reconciler) completeChunk(ctx context.Context, highGroup []kafka.Record) error {
	if err := r.maybeWait(ctx, failpoint.AfterHighMarkerObserved); err != nil {
		return err
	}

	conn, err := r.cpStore.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Apply any account changes that happened to be in the same source
	// transaction as the HIGH marker (they are idempotent, so unconditional
	// application is safe).
	source := highGroup[0].Change.Source
	cdcCount := 0
	for _, rec := range highGroup {
		if rec.Change.Account != nil {
			cdcCount++
			if !r.disableEviction {
				r.mu.Lock()
				delete(r.candidates, rec.Change.Account.ID)
				r.mu.Unlock()
			}
			if err := r.sink.Apply(ctx, tx, rec.Change); err != nil {
				return err
			}
		}
	}
	if cdcCount > 0 {
		r.metrics.RecordCDC(cdcCount)
	}
	if err := r.cpStore.MarkApplied(ctx, tx, r.cfg.JobID, source.Generation, source.LSN, source.XID); err != nil {
		return err
	}

	if err := r.maybeWait(ctx, failpoint.BeforeChunkCommit); err != nil {
		return err
	}

	r.mu.Lock()
	candidateCount := len(r.candidates)
	survivors := make([]model.Account, 0, candidateCount)
	for _, account := range r.candidates {
		survivors = append(survivors, account)
	}
	r.candidates = nil
	r.mu.Unlock()

	if err := r.sink.WriteCandidates(ctx, tx, survivors); err != nil {
		return fmt.Errorf("write survivors: %w", err)
	}

	r.cp.CompletedThrough = r.chunk.Max
	lastOffset := highGroup[len(highGroup)-1].Offset
	if err := r.updateCheckpoint(ctx, tx, lastOffset, source.LSN); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit chunk completion: %w", err)
	}
	if err := r.maybeWait(ctx, failpoint.AfterDestinationCommit); err != nil {
		return err
	}

	log.Printf("seam: chunk %s complete survivors=%d", r.chunk, len(survivors))
	r.metrics.RecordChunk(telemetry.ChunkStats{Candidates: candidateCount, Survivors: len(survivors)})
	return nil
}

// updateCheckpoint mutates the in-memory checkpoint and persists it.
func (r *Reconciler) updateCheckpoint(ctx context.Context, tx pgx.Tx, offset int64, lsn string) error {
	r.cp.NextKafkaOffset = offset + 1
	if lsn != "" {
		r.cp.LastAppliedLSN = lsn
	}
	return r.cpStore.UpdateCheckpoint(ctx, tx, r.cp)
}

// flushBatch applies a batch of records during CDC-only mode.
func (r *Reconciler) flushBatch(ctx context.Context, records []kafka.Record) error {
	groups := groupByTransaction(records)
	for _, group := range groups {
		if err := r.applySourceTransaction(ctx, group, false); err != nil {
			return err
		}
	}
	return nil
}

// groupByTransaction splits records into contiguous groups by source transaction.
func groupByTransaction(records []kafka.Record) [][]kafka.Record {
	if len(records) == 0 {
		return nil
	}
	var groups [][]kafka.Record
	var current []kafka.Record
	var currentSource *model.SourceTx
	for _, rec := range records {
		src := &rec.Change.Source
		if currentSource == nil || *currentSource != *src {
			if len(current) > 0 {
				groups = append(groups, current)
			}
			current = nil
			currentSource = src
		}
		current = append(current, rec)
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups
}

func (r *Reconciler) maybeWait(ctx context.Context, name failpoint.Name) error {
	p := r.failpoints.Get(name)
	if p == nil {
		return nil
	}
	log.Printf("seam: failpoint waiting at %s", p)
	if err := p.Reach(ctx); err != nil {
		return err
	}
	log.Printf("seam: failpoint resumed at %s", p)
	return nil
}


