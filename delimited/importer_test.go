package delimited

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// collectingSink records all committed records in order.
type collectingSink struct {
	mu      sync.Mutex
	records []Record
	failAt  int // fail after this many records (0 = never)
	count   int
}

func (s *collectingSink) Write(ctx context.Context, records []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range records {
		s.count++
		if s.failAt > 0 && s.count > s.failAt {
			return errors.New("sink failure")
		}
		s.records = append(s.records, r)
	}
	return nil
}

func (s *collectingSink) snapshot() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Record, len(s.records))
	copy(out, s.records)
	return out
}

func TestImporterBasic(t *testing.T) {
	input := "a,b,c\n1,2,3\n4,5,6\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].Fields[0] != "1" || recs[1].Fields[0] != "4" {
		t.Fatalf("records out of order: %v", recs)
	}
	stats := im.Stats()
	if stats.RecordsCommitted != 2 {
		t.Fatalf("committed = %d, want 2", stats.RecordsCommitted)
	}
}

func TestImporterNoHeader(t *testing.T) {
	input := "1,2,3\n4,5,6\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	cfg := Config{NoHeaderRow: true}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
}

func TestImporterQuotedNewlines(t *testing.T) {
	input := "\"a\",\"b\"\n\"1\n2\",\"3\"\n\"4\",\"5\"\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{NoHeaderRow: true}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	if recs[1].Fields[0] != "1\n2" {
		t.Fatalf("quoted newline broken: %q", recs[1].Fields[0])
	}
}

func TestImporterColumnMismatchStrict(t *testing.T) {
	input := "a,b,c\n1,2\n3,4,5\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	cfg := Config{Strict: true}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	err = im.Run(context.Background())
	var cm *ErrColumnMismatch
	if !errors.As(err, &cm) {
		t.Fatalf("got %v, want ErrColumnMismatch", err)
	}
}

func TestImporterColumnMismatchPad(t *testing.T) {
	input := "a,b,c\n1,2\n3,4,5\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	cfg := Config{OnMismatch: MismatchPad}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if len(recs[0].Fields) != 3 || recs[0].Fields[2] != "" {
		t.Fatalf("padded record = %v", recs[0].Fields)
	}
	stats := im.Stats()
	if stats.ColumnsPadded != 1 {
		t.Fatalf("padded = %d, want 1", stats.ColumnsPadded)
	}
}

func TestImporterColumnMismatchDrop(t *testing.T) {
	input := "a,b,c\n1,2\n3,4,5\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	cfg := Config{OnMismatch: MismatchDrop}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1 (mismatched dropped)", len(recs))
	}
	stats := im.Stats()
	if stats.RecordsSkipped != 1 {
		t.Fatalf("skipped = %d, want 1", stats.RecordsSkipped)
	}
}

func TestImporterLenientSkipsBadRecords(t *testing.T) {
	input := "a,b\n\"bad\n1,2\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	cfg := Config{NoHeaderRow: true, Strict: false}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	// lenient: unclosed quote at EOF is skipped, good record committed
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
}

func TestImporterShardedOrder(t *testing.T) {
	// Generate a large input to exercise multiple shards.
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for i := 0; i < 1000; i++ {
		sb.WriteString(fmt.Sprintf("%d,user%d\n", i, i))
	}
	src := NewBytesSource([]byte(sb.String()))
	sink := &collectingSink{}
	cfg := Config{ShardCount: 8}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 1000 {
		t.Fatalf("got %d records, want 1000", len(recs))
	}
	for i, r := range recs {
		if r.Fields[0] != fmt.Sprintf("%d", i) {
			t.Fatalf("record %d out of order: %v", i, r.Fields)
		}
	}
}

func TestImporterShardedWithQuotedNewlines(t *testing.T) {
	// Quoted newlines must not break shard boundaries.
	var sb strings.Builder
	sb.WriteString("id,data\n")
	for i := 0; i < 500; i++ {
		if i%3 == 0 {
			sb.WriteString(fmt.Sprintf("%d,\"line1\nline2\"\n", i))
		} else {
			sb.WriteString(fmt.Sprintf("%d,plain\n", i))
		}
	}
	src := NewBytesSource([]byte(sb.String()))
	sink := &collectingSink{}
	cfg := Config{ShardCount: 6}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 500 {
		t.Fatalf("got %d records, want 500", len(recs))
	}
	for i, r := range recs {
		if r.Fields[0] != fmt.Sprintf("%d", i) {
			t.Fatalf("record %d out of order: %v", i, r.Fields)
		}
	}
}

func TestImporterCancel(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for i := 0; i < 10000; i++ {
		sb.WriteString(fmt.Sprintf("%d,user%d\n", i, i))
	}
	src := NewBytesSource([]byte(sb.String()))
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{ShardCount: 4}, sink)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	err = im.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}
