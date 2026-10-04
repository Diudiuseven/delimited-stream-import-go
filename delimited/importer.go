package delimited

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
)

// Sink receives committed records in input order. Write must be safe to call
// concurrently? No: the importer calls Write sequentially from a single
// merger goroutine, so sinks do not need to be concurrency-safe.
type Sink interface {
	Write(ctx context.Context, records []Record) error
}

// SinkFunc adapts a function to the Sink interface.
type SinkFunc func(ctx context.Context, records []Record) error

// Write implements Sink.
func (f SinkFunc) Write(ctx context.Context, records []Record) error {
	return f(ctx, records)
}

// Importer runs a resumable, sharded import over a seekable Source.
type Importer struct {
	src   Source
	cfg   Config
	sink  Sink
	store CheckpointStore

	stats  Stats
	format Format
	schema *Schema

	boundaries  []int64
	startShard  int
	startOffset int64
	startRecord int64
}

// NewImporter returns an Importer over src. When cfg.Format is nil the dialect
// is detected from the input. When cfg.Schema is nil and NoHeaderRow is false
// the first record is consumed as the header row.
func NewImporter(src Source, cfg Config, sink Sink) (*Importer, error) {
	if src == nil {
		return nil, errors.New("delimited: nil source")
	}
	if sink == nil {
		return nil, errors.New("delimited: nil sink")
	}
	cfg = cfg.withDefaults()
	if cfg.ShardCount <= 0 {
		cfg.ShardCount = runtime.GOMAXPROCS(0)
	}
	if cfg.ShardCount < 1 {
		cfg.ShardCount = 1
	}
	im := &Importer{
		src:   src,
		cfg:   cfg,
		sink:  sink,
		store: nil,
	}
	return im, nil
}

// Import runs a complete import over src with the given config and sink. It is
// a convenience wrapper around NewImporter and Run.
func Import(ctx context.Context, src Source, cfg Config, sink Sink) error {
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		return err
	}
	return im.Run(ctx)
}

// SetCheckpointStore enables resumable imports.
func (im *Importer) SetCheckpointStore(store CheckpointStore) {
	im.store = store
}

// Stats returns a snapshot of the import counters.
func (im *Importer) Stats() Stats { return im.stats.Snapshot() }

// Format returns the detected or configured dialect.
func (im *Importer) Format() Format { return im.format }

// Schema returns the schema in use (nil when no header mapping).
func (im *Importer) Schema() *Schema { return im.schema }

// Run executes the import. It returns when all records are committed, when
// the context is cancelled, or when a fatal error occurs. On a fatal error
// no further records are committed; resume by calling Run again with the same
// checkpoint store.
func (im *Importer) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Empty source: nothing to import.
	if im.src.Size() == 0 {
		return nil
	}

	// 1. Load checkpoint if resuming.
	var cp *Checkpoint
	if im.store != nil {
		var err error
		cp, err = im.store.Load()
		if err != nil {
			return err
		}
		if cp != nil && cp.SourceSize != im.src.Size() {
			return &ErrSourceChanged{CheckpointSize: cp.SourceSize, CurrentSize: im.src.Size()}
		}
	}

	// 2. Determine format. An explicit cfg.Format wins; otherwise the
	// checkpoint's format (if resuming) wins; otherwise detect from the
	// sample. Detection is deterministic, but reusing the checkpoint format
	// guarantees the resume uses the same dialect as the first run.
	switch {
	case im.cfg.Format != nil:
		im.format = *im.cfg.Format
	case cp != nil && cp.Format.Delimiter != 0:
		im.format = cp.Format
	default:
		sample := make([]byte, im.cfg.SampleSize)
		n, err := readAtFull(im.src, sample, 0)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
		detected, err := DetectFormatBytes(sample[:n])
		if err != nil {
			return err
		}
		if detected.Uncertain {
			return &ErrUncertainFormat{Reasons: detected.Reasons}
		}
		im.format = *detected
	}

	// 3. Build schema from the header row if needed.
	headerEnd := int64(0)
	if im.cfg.Schema != nil {
		im.schema = im.cfg.Schema
	} else if !im.cfg.NoHeaderRow {
		r := io.NewSectionReader(im.src, 0, im.src.Size())
		hcfg := im.cfg
		hcfg.Format = &im.format
		hcfg.Schema = nil
		hcfg.NoHeaderRow = false
		rd, err := NewReader(r, hcfg, &im.stats)
		if err != nil {
			return err
		}
		im.schema = rd.Schema()
		headerEnd = rd.BytesRead()
	}

	// 4. Compute shard boundaries over the data region.
	dataSize := im.src.Size() - headerEnd
	boundaries, err := shardBoundaries(
		offsetSource{src: im.src, offset: headerEnd, size: dataSize},
		im.cfg.ShardCount, im.format)
	if err != nil {
		return err
	}
	// translate back to absolute offsets
	for i := range boundaries {
		boundaries[i] += headerEnd
	}
	im.boundaries = boundaries

	// 5. Resume position from checkpoint.
	im.startShard = 0
	im.startOffset = headerEnd
	im.startRecord = 0
	if cp != nil && cp.Offset > headerEnd && cp.Offset <= im.src.Size() {
		im.startOffset = cp.Offset
		im.startRecord = cp.RecordIndex
	}
	// Find the shard containing the start offset.
	for i := 0; i < len(boundaries)-1; i++ {
		if im.startOffset < boundaries[i+1] {
			im.startShard = i
			break
		}
		im.startShard = i + 1
	}
	if im.startOffset >= boundaries[len(boundaries)-1] {
		// already complete
		return nil
	}

	// 6. Launch shard workers.
	const chanBuf = 256
	channels := make([]chan Record, len(boundaries)-1)
	for i := range channels {
		channels[i] = make(chan Record, chanBuf)
	}
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < len(boundaries)-1; i++ {
		if i < im.startShard {
			continue
		}
		start := boundaries[i]
		if i == im.startShard {
			start = im.startOffset
		}
		wg.Add(1)
		go func(i int, start int64) {
			defer wg.Done()
			defer close(channels[i])
			im.runShard(ctx, int64(i), start, boundaries[i+1], channels[i], errCh)
		}(i, start)
	}

	// 7. Merge shards in input order and commit.
	mergeErr := im.merge(ctx, channels, errCh)

	// 8. Wait for workers to drain.
	wg.Wait()
	cancel()

	// Surface a worker error if the merger did not already fail.
	if mergeErr == nil {
		select {
		case err := <-errCh:
			if err != nil {
				mergeErr = err
			}
		default:
		}
	}
	return mergeErr
}

// merge consumes shard channels in order, batches records and commits them.
func (im *Importer) merge(ctx context.Context, channels []chan Record, errCh <-chan error) error {
	batch := make([]Record, 0, im.cfg.BatchSize)
	shard := im.startShard
	offset := im.startOffset
	record := im.startRecord

	commit := func() error {
		if len(batch) == 0 {
			return nil
		}
		for i := range batch {
			batch[i].Index = record + int64(i)
		}
		if err := im.sink.Write(ctx, batch); err != nil {
			return err
		}
		im.stats.addCommitted(int64(len(batch)))
		record += int64(len(batch))
		last := batch[len(batch)-1]
		offset = last.EndPos.Offset
		shard = im.shardAt(offset)
		if im.store != nil {
			cp := &Checkpoint{
				Format:      im.format,
				ShardCount:  im.cfg.ShardCount,
				ShardIndex:  shard,
				Offset:      offset,
				RecordIndex: record,
				SourceSize:  im.src.Size(),
			}
			if err := im.store.Save(cp); err != nil {
				return err
			}
		}
		batch = batch[:0]
		return nil
	}

	for i := im.startShard; i < len(channels); i++ {
		ch := channels[i]
	shardLoop:
		for {
			select {
			case <-ctx.Done():
				_ = commit()
				return ctx.Err()
			case err := <-errCh:
				if err != nil {
					_ = commit()
					return err
				}
			case rec, ok := <-ch:
				if !ok {
					if err := commit(); err != nil {
						return err
					}
					im.stats.addShard()
					break shardLoop
				}
				batch = append(batch, rec)
				if len(batch) >= im.cfg.BatchSize {
					if err := commit(); err != nil {
						return err
					}
				}
			}
		}
	}
	return commit()
}

// shardAt returns the shard index containing the given offset.
func (im *Importer) shardAt(offset int64) int {
	for i := 0; i < len(im.boundaries)-1; i++ {
		if offset < im.boundaries[i+1] {
			return i
		}
	}
	return len(im.boundaries) - 2
}

// runShard processes one shard, sending records to ch until the shard ends or
// a fatal error occurs.
func (im *Importer) runShard(ctx context.Context, shardIdx int64, start, end int64, ch chan<- Record, errCh chan<- error) {
	r := io.NewSectionReader(im.src, start, end-start)
	rd, err := NewReader(r, im.readerConfig(shardIdx), &im.stats)
	if err != nil {
		select {
		case errCh <- err:
		case <-ctx.Done():
		}
		return
	}
	rd.SetBaseOffset(start)
	for {
		rec, err := rd.Read()
		if err == io.EOF {
			return
		}
		if err != nil {
			if im.cfg.Strict {
				select {
				case errCh <- err:
				case <-ctx.Done():
				}
				return
			}
			// Lenient: the reader already counted and skipped the record.
			select {
			case <-ctx.Done():
				return
			default:
			}
			continue
		}
		select {
		case ch <- rec:
		case <-ctx.Done():
			return
		}
	}
}

// readerConfig returns a Config for a shard reader: explicit format, shared
// schema, no header row.
func (im *Importer) readerConfig(shardIdx int64) Config {
	cfg := im.cfg
	cfg.Format = &im.format
	cfg.Schema = im.schema
	cfg.NoHeaderRow = true
	return cfg
}

// readAtFull reads up to len(p) bytes from src at offset, like io.ReadFull
// but tolerant of EOF after at least one byte.
func readAtFull(src Source, p []byte, off int64) (int, error) {
	n, err := src.ReadAt(p, off)
	if err == io.EOF && n > 0 {
		return n, nil
	}
	if err == io.ErrUnexpectedEOF {
		return n, nil
	}
	return n, err
}

// offsetSource presents a sub-range of a Source as a Source.
type offsetSource struct {
	src    Source
	offset int64
	size   int64
}

func (o offsetSource) ReadAt(p []byte, off int64) (int, error) {
	return o.src.ReadAt(p, off+o.offset)
}

func (o offsetSource) Size() int64 { return o.size }
