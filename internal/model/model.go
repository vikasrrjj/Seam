// Package model contains the small, explicit data types shared across Seam.
package model

import "fmt"

type Operation string

const (
	OpInsert Operation = "insert"
	OpUpdate Operation = "update"
	OpDelete Operation = "delete"
)

// Account is the destination shape of the accounts table.
type Account struct {
	ID           int64
	Owner        string
	BalanceCents int64
}

// Marker is a control record published through the same CDC stream as accounts.
type Marker struct {
	ID       string
	Kind     MarkerKind // low or high
	JobID    string
	Attempt  string
	ChunkMin int64
	ChunkMax int64
}

type MarkerKind string

const (
	MarkerLow  MarkerKind = "low"
	MarkerHigh MarkerKind = "high"
)

// SourceTx identifies a committed source transaction. It is stable across
// Kafka republishes: the same PostgreSQL commit LSN is republished at a new
// Kafka offset after source-reader recovery, so dedupe must use SourceTx, not
// the Kafka offset.
type SourceTx struct {
	Generation string // logical generation of the replication pipeline
	XID        uint32 // source transaction id (informational)
	LSN        string // source commit LSN
}

func (s SourceTx) String() string {
	return fmt.Sprintf("%s:%s", s.Generation, s.LSN)
}

// Change is one record on the ordered Kafka stream.
type Change struct {
	Op      Operation
	Account *Account // non-nil for accounts changes
	Marker  *Marker  // non-nil for marker changes
	Source  SourceTx
}

func (c Change) Key() string {
	switch {
	case c.Account != nil:
		return fmt.Sprintf("account:%d", c.Account.ID)
	case c.Marker != nil:
		return fmt.Sprintf("marker:%s", c.Marker.ID)
	default:
		return "unknown"
	}
}

// JobConfig identifies one backfill job.
type JobConfig struct {
	JobID           string
	SourceDSN       string
	SourceReplDSN   string
	SourceSlot      string
	SourcePublication string
	DestDSN         string
	KafkaBrokers    []string
	KafkaTopic      string
	ChunkSize       int
}

// Checkpoint is the durable progress state for a job.
type Checkpoint struct {
	JobID            string
	Generation       string
	Attempt          string
	ScanUpperBound   int64
	CompletedThrough int64 // all IDs <= this are fully backfilled
	NextKafkaOffset  int64
	LastAppliedLSN   string
	Active           bool
}

// ChunkRange is an inclusive primary-key interval.
type ChunkRange struct {
	Min int64
	Max int64
}

func (c ChunkRange) String() string {
	return fmt.Sprintf("[%d,%d]", c.Min, c.Max)
}
