package delimited

import (
	"context"
	"io"
)

// StreamImporter runs a sequential, non-resumable import over an io.Reader.
// It is the streaming entry point for inputs that are not seekable (pipes,
// network streams, compressed readers). For sharded, resumable imports use
// Importer with a Source.
type StreamImporter struct {
	r    io.Reader
	cfg  Config
	sink Sink

	stats  Stats
	format Format
	schema *Schema
}

// NewStreamImporter returns a StreamImporter over r. When cfg.Format is nil
// the dialect is detected from the input sample. When cfg.Schema is nil and
// NoHeaderRow is false the first record is consumed as the header row.
func NewStreamImporter(r io.Reader, cfg Config, sink Sink) (*StreamImporter, error) {
	if r == nil {
		return nil, ErrNilReader
	}
	if sink == nil {
		return nil, ErrNilSink
	}
	cfg = cfg.withDefaults()
	return &StreamImporter{r: r, cfg: cfg, sink: sink}, nil
}

// Stats returns a snapshot of the import counters.
func (im *StreamImporter) Stats() Stats { return im.stats.Snapshot() }

// Format returns the detected or configured dialect.
func (im *StreamImporter) Format() Format { return im.format }

// Schema returns the schema in use (nil when no header mapping).
func (im *StreamImporter) Schema() *Schema { return im.schema }

// Run executes the import. It returns when all records are committed, when the
// context is cancelled, or when a fatal error occurs.
func (im *StreamImporter) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rd, err := NewReader(im.r, im.cfg, &im.stats)
	if err != nil {
		return err
	}
	im.format = rd.Format()
	im.schema = rd.Schema()

	batch := make([]Record, 0, im.cfg.BatchSize)
	committed := int64(0)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		for i := range batch {
			batch[i].Index = committed + int64(i)
		}
		if err := im.sink.Write(ctx, batch); err != nil {
			return err
		}
		im.stats.addCommitted(int64(len(batch)))
		committed += int64(len(batch))
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			_ = flush()
			return ctx.Err()
		default:
		}
		rec, err := rd.Read()
		if err == io.EOF {
			return flush()
		}
		if err != nil {
			if im.cfg.Strict {
				_ = flush()
				return err
			}
			// Lenient: the reader already counted and skipped the record.
			continue
		}
		batch = append(batch, rec)
		if len(batch) >= im.cfg.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// ErrNilReader is returned when a nil reader is passed to NewStreamImporter.
var ErrNilReader = &simpleError{"delimited: nil reader"}

// ErrNilSink is returned when a nil sink is passed to an importer.
var ErrNilSink = &simpleError{"delimited: nil sink"}

type simpleError struct{ msg string }

func (e *simpleError) Error() string { return e.msg }
