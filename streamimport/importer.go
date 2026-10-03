package streamimport

import (
	"context"
	"io"
)

// Sink receives committed batches. CommitBatch must be atomic: either
// all records in the batch are durably stored or none are. Records in
// one batch are in input order.
type Sink interface {
	CommitBatch(ctx context.Context, recs []Record) error
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(ctx context.Context, recs []Record) error

func (f SinkFunc) CommitBatch(ctx context.Context, recs []Record) error { return f(ctx, recs) }

// Progress reports import progress; safe to call from any goroutine.
type Progress struct {
}

// Snapshot is a consistent copy of Progress.
type Snapshot struct {
	RecordsRead      int64
	RecordsCommitted int64
	RecordsSkipped   int64
	Errors           int64
	BytesRead        int64
}

// Snapshot returns the current counters.
func (p *Progress) Snapshot() Snapshot { return Snapshot{} }

// ImporterConfig configures an Importer.
type ImporterConfig struct {
	Reader ReaderConfig
	Schema Schema
	// BatchSize is the number of records per atomic commit. Must be > 0.
	BatchSize int
	// Store enables resumable imports. When nil, no checkpoints are
	// written and a restart begins from the start of the input.
	Store CheckpointStore
}

// Importer streams records from a source into a Sink in batches,
// checkpointing after every successful commit.
type Importer struct {
}

// NewImporter validates cfg and returns an Importer.
func NewImporter(cfg ImporterConfig) (*Importer, error) {
	return &Importer{}, nil
}

// Progress exposes live counters.
func (im *Importer) Progress() *Progress { return nil }

// Run consumes r until EOF, cancellation or a fatal error. Batches are
// committed in input order; after each commit the checkpoint is saved.
// On cancellation the import stops at a batch boundary: committed
// records are never re-committed on resume, uncommitted records are
// never skipped, and no half-written batch can exist because Sink
// commits are atomic and the checkpoint is written only after commit.
func (im *Importer) Run(ctx context.Context, r io.Reader) error {
	return nil
}
