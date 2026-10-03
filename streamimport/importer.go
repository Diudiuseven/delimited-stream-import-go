package streamimport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync/atomic"
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

// Progress reports import progress; safe to observe from any goroutine.
type Progress struct {
	read      atomic.Int64
	committed atomic.Int64
	skipped   atomic.Int64
	errs      atomic.Int64
	bytesRead atomic.Int64
}

// Snapshot is a consistent copy of Progress counters.
type Snapshot struct {
	RecordsRead      int64
	RecordsCommitted int64
	RecordsSkipped   int64
	Errors           int64
	BytesRead        int64
}

// Snapshot returns the current counters.
func (p *Progress) Snapshot() Snapshot {
	return Snapshot{
		RecordsRead:      p.read.Load(),
		RecordsCommitted: p.committed.Load(),
		RecordsSkipped:   p.skipped.Load(),
		Errors:           p.errs.Load(),
		BytesRead:        p.bytesRead.Load(),
	}
}

// ImporterConfig configures an Importer.
type ImporterConfig struct {
	Reader ReaderConfig
	Schema Schema
	// Sink receives committed batches. Required.
	Sink Sink
	// BatchSize is the number of records per atomic commit. Must be > 0.
	BatchSize int
	// Store enables resumable imports. When nil, no checkpoints are
	// written and a restart begins from the start of the input.
	Store CheckpointStore
}

// Importer streams records from a source into a Sink in batches,
// checkpointing after every successful commit.
type Importer struct {
	cfg      ImporterConfig
	progress *Progress
}

// NewImporter validates cfg and returns an Importer.
func NewImporter(cfg ImporterConfig) (*Importer, error) {
	if cfg.BatchSize <= 0 {
		return nil, fmt.Errorf("streamimport: BatchSize must be > 0, got %d", cfg.BatchSize)
	}
	if cfg.Reader.MaxRecordBytes <= 0 {
		return nil, fmt.Errorf("streamimport: MaxRecordBytes must be > 0")
	}
	if cfg.Reader.Dialect.Delimiter == 0 {
		return nil, fmt.Errorf("streamimport: dialect delimiter is not set")
	}
	if cfg.Sink == nil {
		return nil, fmt.Errorf("streamimport: Sink is required")
	}
	return &Importer{cfg: cfg, progress: &Progress{}}, nil
}

// Progress exposes live counters.
func (im *Importer) Progress() *Progress { return im.progress }

// Run consumes r until EOF, cancellation or a fatal error. Batches are
// committed in input order; after each commit the checkpoint is saved.
//
// Recovery semantics: on cancellation, timeout or a fatal error the
// import stops at a batch boundary. Because Sink.CommitBatch is atomic
// and the checkpoint is written only after a successful commit, a
// restart that seeks to the checkpoint offset never re-commits
// committed records, never skips uncommitted ones, and never observes
// a half-written batch.
func (im *Importer) Run(ctx context.Context, r io.Reader) error {
	return im.run(ctx, r)
}

// run processes the stream, resuming from the checkpoint when a store
// is configured.
func (im *Importer) run(ctx context.Context, r io.Reader) error {
	cfg := im.cfg
	var startCp Checkpoint
	if cfg.Store != nil {
		cp, err := cfg.Store.Load(ctx)
		if err != nil {
			return fmt.Errorf("streamimport: load checkpoint: %w", err)
		}
		startCp = cp
	}
	resumed := false
	if startCp.Offset > 0 {
		skipped, err := skipToOffset(r, startCp.Offset)
		if err != nil {
			return fmt.Errorf("streamimport: resume at offset %d: %w", startCp.Offset, err)
		}
		r = skipped
		resumed = true
	}

	readerCfg := cfg.Reader
	// Mid-stream starts must not strip a BOM: the bytes at the resume
	// offset are record content, not a file header.
	readerCfg.noBOM = resumed
	// Wire lenient-mode error reporting through the progress counters.
	userOnError := readerCfg.OnError
	readerCfg.OnError = func(err error) {
		im.progress.skipped.Add(1)
		im.progress.errs.Add(1)
		if userOnError != nil {
			userOnError(err)
		}
	}
	rd, err := NewReader(r, readerCfg)
	if err != nil {
		return err
	}
	if startCp.Offset > 0 {
		rd.off = startCp.Offset
	}

	schema := cfg.Schema
	committed := startCp.Records

	// Consume the header row when the dialect declares one. On resume
	// (checkpoint offset > 0) the header was already consumed by the
	// previous run and must not be re-read.
	if cfg.Reader.Dialect.HasHeader && startCp.Offset == 0 {
		hdr, err := rd.Read()
		if err != nil && err != io.EOF {
			return err
		}
		if err == nil {
			if err := schema.ApplyHeader(hdr.Fields); err != nil {
				return err
			}
		}
	}

	batch := make([]Record, 0, cfg.BatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := cfg.Sink.CommitBatch(ctx, batch); err != nil {
			return err
		}
		last := batch[len(batch)-1]
		committed += int64(len(batch))
		im.progress.committed.Add(int64(len(batch)))
		if cfg.Store != nil {
			if err := cfg.Store.Save(ctx, Checkpoint{Offset: last.EndOffset, Records: committed}); err != nil {
				return fmt.Errorf("streamimport: save checkpoint: %w", err)
			}
		}
		batch = batch[:0]
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			// Stop at a batch boundary: uncommitted records stay
			// uncommitted and will be re-read on resume.
			return err
		}
		rec, err := rd.Read()
		if err == io.EOF {
			return flush()
		}
		if err != nil {
			im.progress.errs.Add(1)
			return err
		}
		im.progress.read.Add(1)
		im.progress.bytesRead.Store(rec.EndOffset)

		norm, nerr := schema.Normalize(rec)
		if nerr != nil {
			im.progress.errs.Add(1)
			if schema.Policy == RaggedDrop || cfg.Reader.Lenient {
				im.progress.skipped.Add(1)
				if userOnError != nil {
					userOnError(nerr)
				}
				continue
			}
			return nerr
		}
		batch = append(batch, norm)
		if len(batch) >= cfg.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// skipToOffset positions the stream at the given post-BOM offset and
// returns a reader for the remainder. Offsets are relative to the first
// byte after any BOM, so a leading BOM is accounted for before seeking.
func skipToOffset(r io.Reader, off int64) (io.Reader, error) {
	br := bufio.NewReader(r)
	bomLen := int64(0)
	if b, err := br.Peek(3); err == nil && b[0] == utf8BOM[0] && b[1] == utf8BOM[1] && b[2] == utf8BOM[2] {
		bomLen = 3
	}
	if s, ok := r.(io.Seeker); ok {
		// Absolute file position = BOM length + post-BOM offset.
		if _, err := s.Seek(bomLen+off, io.SeekStart); err != nil {
			return nil, err
		}
		rs, ok := r.(io.Reader)
		if !ok {
			return nil, fmt.Errorf("streamimport: seekable input is not readable")
		}
		return rs, nil
	}
	if bomLen > 0 {
		if _, err := br.Discard(int(bomLen)); err != nil {
			return nil, err
		}
	}
	if _, err := io.CopyN(io.Discard, br, off); err != nil {
		return nil, err
	}
	return br, nil
}
