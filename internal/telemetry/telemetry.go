// Package telemetry exposes lightweight runtime metrics for a Seam job.
package telemetry

import (
	"sync"
	"sync/atomic"
)

// Metrics holds counters updated by the reconciler.
type Metrics struct {
	chunksCompleted  atomic.Int64
	candidatesSeen   atomic.Int64
	survivorsWritten atomic.Int64
	cdcEventsApplied atomic.Int64
	// per-chunk callback is invoked under the reconciler lock after each chunk.
	mu              sync.RWMutex
	onChunkComplete func(ChunkStats)
}

// ChunkStats describes a completed reconciliation window.
type ChunkStats struct {
	Candidates int
	Survivors  int
}

// NewMetrics creates a metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// OnChunkComplete registers a callback invoked after each chunk commits.
func (m *Metrics) OnChunkComplete(fn func(ChunkStats)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onChunkComplete = fn
}

// RecordChunk updates counters and invokes the optional callback.
func (m *Metrics) RecordChunk(stats ChunkStats) {
	m.chunksCompleted.Add(1)
	m.candidatesSeen.Add(int64(stats.Candidates))
	m.survivorsWritten.Add(int64(stats.Survivors))
	m.mu.RLock()
	fn := m.onChunkComplete
	m.mu.RUnlock()
	if fn != nil {
		fn(stats)
	}
}

// RecordCDC increments the CDC event counter.
func (m *Metrics) RecordCDC(n int) {
	m.cdcEventsApplied.Add(int64(n))
}

// ChunksCompleted returns the number of completed chunks.
func (m *Metrics) ChunksCompleted() int64 { return m.chunksCompleted.Load() }

// CandidatesSeen returns the number of historical rows read as candidates.
func (m *Metrics) CandidatesSeen() int64 { return m.candidatesSeen.Load() }

// SurvivorsWritten returns the number of candidates that survived eviction.
func (m *Metrics) SurvivorsWritten() int64 { return m.survivorsWritten.Load() }

// CDCEventsApplied returns the number of live CDC events applied.
func (m *Metrics) CDCEventsApplied() int64 { return m.cdcEventsApplied.Load() }
