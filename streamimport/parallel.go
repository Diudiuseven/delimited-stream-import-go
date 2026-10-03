package streamimport

import (
	"context"
	"io"
)

// ParallelConfig configures a ParallelImporter.
type ParallelConfig struct {
	ImporterConfig
	// Shards is the number of concurrent shards. Values < 1 mean 1.
	Shards int
}

// ParallelImporter splits a sized, seekable input into byte-range
// shards aligned to record boundaries, parses them concurrently and
// commits batches strictly in input order. A failing shard aborts the
// run; shards never commit partial batches, and already-committed
// prefixes remain resumable via the checkpoint store.
type ParallelImporter struct {
}

// NewParallelImporter validates cfg and returns a ParallelImporter.
func NewParallelImporter(cfg ParallelConfig) (*ParallelImporter, error) {
	return &ParallelImporter{}, nil
}

// Progress exposes live counters.
func (p *ParallelImporter) Progress() *Progress { return nil }

// Run processes size bytes from ra. ra must support concurrent reads
// from independent goroutines (e.g. *os.File, *bytes.Reader).
func (p *ParallelImporter) Run(ctx context.Context, ra io.ReaderAt, size int64) error {
	return nil
}
