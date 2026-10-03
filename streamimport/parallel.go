package streamimport

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// shardResult records a shard worker's terminal error, if any.
type shardResult struct{ err error }

// ParallelConfig configures a ParallelImporter.
type ParallelConfig struct {
	ImporterConfig
	// Shards is the number of concurrent shards. Values < 1 mean 1.
	Shards int
}

// ParallelImporter splits a sized, random-access input into byte-range
// shards aligned to record boundaries, parses them concurrently and
// commits batches strictly in input order.
//
// Shard boundaries are located by scanning for record terminators
// while tracking quote/escape state, and each candidate boundary is
// validated by tentatively parsing a record from it. For pathological
// inputs (e.g. very large quoted fields containing many newlines) use
// Shards == 1 for a fully sequential, exact scan.
//
// Failure semantics: a failing shard aborts the run. Only complete
// batches forming a valid input prefix are ever committed, so a failed
// shard never produces a partial commit; the committed prefix remains
// resumable through the checkpoint store.
type ParallelImporter struct {
	cfg      ParallelConfig
	progress *Progress
}

// NewParallelImporter validates cfg and returns a ParallelImporter.
func NewParallelImporter(cfg ParallelConfig) (*ParallelImporter, error) {
	if cfg.Shards < 1 {
		cfg.Shards = 1
	}
	if _, err := NewImporter(cfg.ImporterConfig); err != nil {
		return nil, err
	}
	return &ParallelImporter{cfg: cfg, progress: &Progress{}}, nil
}

// Progress exposes live counters.
func (p *ParallelImporter) Progress() *Progress { return p.progress }

// Run processes size bytes from ra. ra must support concurrent reads
// from independent goroutines (e.g. *os.File, *bytes.Reader).
func (p *ParallelImporter) Run(ctx context.Context, ra io.ReaderAt, size int64) error {
	cfg := p.cfg
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Account for a leading BOM: all offsets below are in post-BOM
	// space; absolute file offset = bomLen + offset.
	bomLen, err := detectBOMLen(ra, size)
	if err != nil {
		return err
	}
	logical := size - bomLen // post-BOM byte count

	var startCp Checkpoint
	if cfg.Store != nil {
		cp, err := cfg.Store.Load(ctx)
		if err != nil {
			return fmt.Errorf("streamimport: load checkpoint: %w", err)
		}
		startCp = cp
	}

	schema := cfg.Schema
	dataStart := startCp.Offset
	committed := startCp.Records

	// Parse the header up front when starting from the beginning.
	if cfg.Reader.Dialect.HasHeader && startCp.Offset == 0 {
		hdrCfg := cfg.Reader
		hdr, err := readOneRecord(ra, bomLen, logical, hdrCfg)
		if err != nil && err != io.EOF {
			return fmt.Errorf("streamimport: read header: %w", err)
		}
		if err == nil {
			if err := schema.ApplyHeader(hdr.Fields); err != nil {
				return err
			}
			dataStart = hdr.EndOffset
		}
	}
	if dataStart >= logical {
		return nil // nothing to do
	}

	// Compute shard boundaries in post-BOM offset space.
	bounds, err := p.shardBounds(ra, bomLen, dataStart, logical, schema.Width())
	if err != nil {
		return err
	}
	n := len(bounds) - 1

	// Wire per-record error reporting through the progress counters.
	userOnError := cfg.Reader.OnError
	readerCfg := cfg.Reader
	readerCfg.OnError = func(err error) {
		p.progress.skipped.Add(1)
		p.progress.errs.Add(1)
		if userOnError != nil {
			userOnError(err)
		}
	}

	var (
		wg      sync.WaitGroup
		results = make([]shardResult, n)
		chans   = make([]chan Record, n)
	)
	for i := 0; i < n; i++ {
		chans[i] = make(chan Record, cfg.BatchSize)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer close(chans[i])
			results[i].err = p.runShard(ctx, ra, bomLen, logical,
				bounds[i], bounds[i+1], i == n-1, readerCfg, &schema, chans[i])
		}(i)
	}

	// Committer: consume shards in order, commit fixed-size batches.
	commitErr := p.commitLoop(ctx, cfg, chans, results, &committed, cancel)

	wg.Wait()
	if commitErr != nil {
		return commitErr
	}
	for i := range results {
		if results[i].err != nil {
			return results[i].err
		}
	}
	return nil
}

// runShard parses records whose start offset lies in [start, end)
// (the last shard runs to EOF) and forwards them to out.
func (p *ParallelImporter) runShard(ctx context.Context, ra io.ReaderAt, bomLen, logical, start, end int64, last bool, readerCfg ReaderConfig, schema *Schema, out chan<- Record) error {
	sr := io.NewSectionReader(ra, bomLen+start, logical-start)
	rd, err := NewReader(sr, readerCfg)
	if err != nil {
		return err
	}
	for {
		rec, err := rd.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			p.progress.errs.Add(1)
			return err
		}
		rec.Offset += start
		rec.EndOffset += start
		if !last && rec.Offset >= end {
			return nil // belongs to the next shard
		}
		p.progress.read.Add(1)
		p.progress.bytesRead.Store(rec.EndOffset)

		norm, nerr := schema.Normalize(rec)
		if nerr != nil {
			p.progress.errs.Add(1)
			if schema.Policy == RaggedDrop || readerCfg.Lenient {
				p.progress.skipped.Add(1)
				if readerCfg.OnError != nil {
					readerCfg.OnError(nerr)
				}
				continue
			}
			return nerr
		}
		select {
		case out <- norm:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// commitLoop consumes shard channels in order and commits batches.
// It stops committing as soon as any shard has failed, discarding any
// partial batch, so a failed shard never yields a partial commit.
func (p *ParallelImporter) commitLoop(ctx context.Context, cfg ParallelConfig, chans []chan Record, results []shardResult, committed *int64, cancel context.CancelFunc) error {
	batch := make([]Record, 0, cfg.BatchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := cfg.Sink.CommitBatch(ctx, batch); err != nil {
			cancel()
			return err
		}
		*committed += int64(len(batch))
		p.progress.committed.Add(int64(len(batch)))
		if cfg.Store != nil {
			cp := Checkpoint{Offset: batch[len(batch)-1].EndOffset, Records: *committed}
			if err := cfg.Store.Save(ctx, cp); err != nil {
				cancel()
				return fmt.Errorf("streamimport: save checkpoint: %w", err)
			}
		}
		batch = batch[:0]
		return nil
	}

	for i, ch := range chans {
		for rec := range ch {
			batch = append(batch, rec)
			if len(batch) >= cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		// Channel closed: the shard finished. If it failed, discard
		// the partial batch and stop — later shards' records must not
		// be committed past the gap.
		if results[i].err != nil {
			cancel()
			return results[i].err
		}
	}
	return flush()
}

// shardBounds computes n+1 boundaries in post-BOM offset space.
// width is the expected field count per record (0 if unknown), used to
// validate candidate boundaries.
func (p *ParallelImporter) shardBounds(ra io.ReaderAt, bomLen, start, logical int64, width int) ([]int64, error) {
	n := p.cfg.Shards
	span := logical - start
	if span <= 0 {
		return []int64{start, logical}, nil
	}
	if int64(n) > span {
		n = int(span)
		if n < 1 {
			n = 1
		}
	}
	bounds := make([]int64, 0, n+1)
	bounds = append(bounds, start)
	for i := 1; i < n; i++ {
		nominal := start + span*int64(i)/int64(n)
		b, err := p.alignOffset(ra, bomLen, nominal, logical, width)
		if err != nil {
			return nil, err
		}
		if b <= bounds[len(bounds)-1] {
			continue // collapsed shard; skip duplicate boundary
		}
		bounds = append(bounds, b)
	}
	if bounds[len(bounds)-1] != logical {
		bounds = append(bounds, logical)
	}
	return bounds, nil
}

// alignOffset finds the first record boundary at or after pos (post-BOM
// offset space). A candidate boundary is validated by tentatively
// parsing one record from it; candidates that fail to parse are
// skipped, which rejects boundaries landing inside quoted fields.
func (p *ParallelImporter) alignOffset(ra io.ReaderAt, bomLen, pos, logical int64, width int) (int64, error) {
	const chunk = 32 * 1024
	d := p.cfg.Reader.Dialect
	inQuote := false
	for base := pos; base < logical; {
		n := int64(chunk)
		if base+n > logical {
			n = logical - base
		}
		buf := make([]byte, n)
		if _, err := ra.ReadAt(buf, bomLen+base); err != nil && err != io.EOF {
			return 0, err
		}
		for i := 0; i < len(buf); i++ {
			c := buf[i]
			if inQuote {
				if d.Escape != 0 && c == d.Escape {
					i++ // skip escaped byte
					continue
				}
				if c == d.Quote {
					if d.Escape == 0 && i+1 < len(buf) && buf[i+1] == d.Quote {
						i++ // doubled quote
						continue
					}
					inQuote = false
				}
				continue
			}
			if d.Quote != 0 && c == d.Quote {
				inQuote = true
				continue
			}
			if c == '\n' || c == '\r' {
				cand := base + int64(i) + 1
				if c == '\r' && cand < logical {
					var nb [1]byte
					if _, err := ra.ReadAt(nb[:], bomLen+cand); err == nil && nb[0] == '\n' {
						cand++
					}
				}
				if p.validBoundary(ra, bomLen, cand, logical, width) {
					return cand, nil
				}
			}
		}
		base += int64(len(buf))
	}
	return logical, nil
}

// validBoundary reports whether off looks like a real record boundary:
// up to 3 consecutive records must parse cleanly from it, and when the
// expected field count is known each record must match it. This rejects
// candidate boundaries that land inside quoted fields, where parsing
// either fails outright or yields a wrong field count.
func (p *ParallelImporter) validBoundary(ra io.ReaderAt, bomLen, off, logical int64, width int) bool {
	if off >= logical {
		return true
	}
	sr := io.NewSectionReader(ra, bomLen+off, logical-off)
	rd, err := NewReader(sr, p.cfg.Reader)
	if err != nil {
		return false
	}
	for i := 0; i < 3; i++ {
		rec, err := rd.Read()
		if err == io.EOF {
			return true
		}
		if err != nil {
			return false
		}
		if width > 0 && len(rec.Fields) != width {
			return false
		}
	}
	return true
}

// readOneRecord parses a single record starting at absolute offset abs.
func readOneRecord(ra io.ReaderAt, abs, length int64, cfg ReaderConfig) (Record, error) {
	sr := io.NewSectionReader(ra, abs, length)
	rd, err := NewReader(sr, cfg)
	if err != nil {
		return Record{}, err
	}
	return rd.Read()
}

// detectBOMLen returns 3 when the input starts with a UTF-8 BOM.
func detectBOMLen(ra io.ReaderAt, size int64) (int64, error) {
	if size < 3 {
		return 0, nil
	}
	var b [3]byte
	if _, err := ra.ReadAt(b[:], 0); err != nil {
		return 0, err
	}
	if b == [3]byte(utf8BOM) {
		return 3, nil
	}
	return 0, nil
}
