package streamimport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// memSink collects committed batches; each CommitBatch is atomic.
type memSink struct {
	mu      sync.Mutex
	batches [][]Record
	// failAfter, when >= 0, makes the (failAfter+1)-th commit fail.
	failAfter int
	calls     int
}

func (s *memSink) CommitBatch(ctx context.Context, recs []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failAfter >= 0 && s.calls > s.failAfter {
		return errors.New("injected sink failure")
	}
	cp := make([]Record, len(recs))
	copy(cp, recs)
	s.batches = append(s.batches, cp)
	return nil
}

func (s *memSink) all() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Record
	for _, b := range s.batches {
		out = append(out, b...)
	}
	return out
}

func testImporterCfg(sink Sink, store CheckpointStore) ImporterConfig {
	return ImporterConfig{
		Reader: ReaderConfig{
			Dialect:        Dialect{Delimiter: ',', Quote: '"', HasHeader: true},
			MaxRecordBytes: 4096,
		},
		Schema:    Schema{Policy: RaggedReject},
		BatchSize: 2,
		Sink:      sink,
		Store:     store,
	}
}

func makeCSV(rows int) string {
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&sb, "%d,name%d\n", i, i)
	}
	return sb.String()
}

func TestSequentialImport(t *testing.T) {
	sink := &memSink{failAfter: -1}
	im, err := NewImporter(testImporterCfg(sink, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background(), strings.NewReader(makeCSV(5))); err != nil {
		t.Fatal(err)
	}
	recs := sink.all()
	if len(recs) != 5 {
		t.Fatalf("got %d records", len(recs))
	}
	if recs[0].Fields[0] != "0" || recs[4].Fields[1] != "name4" {
		t.Fatalf("unexpected records: %v %v", recs[0], recs[4])
	}
	snap := im.Progress().Snapshot()
	if snap.RecordsCommitted != 5 || snap.RecordsRead != 5 {
		t.Fatalf("bad progress: %+v", snap)
	}
	// Batches of 2: 2,2,1.
	if len(sink.batches) != 3 || len(sink.batches[0]) != 2 || len(sink.batches[2]) != 1 {
		t.Fatalf("bad batching: %v", sink.batches)
	}
}

func TestResumeAfterSinkFailure(t *testing.T) {
	store := NewMemoryCheckpointStore()
	input := makeCSV(6)

	// First run: fail on the 2nd commit (records 0-1 committed).
	sink1 := &memSink{failAfter: 1}
	im1, err := NewImporter(testImporterCfg(sink1, store))
	if err != nil {
		t.Fatal(err)
	}
	if err := im1.Run(context.Background(), strings.NewReader(input)); err == nil {
		t.Fatal("expected failure")
	}
	if got := len(sink1.all()); got != 2 {
		t.Fatalf("first run committed %d records, want 2", got)
	}
	cp, _ := store.Load(context.Background())
	if cp.Records != 2 || cp.Offset == 0 {
		t.Fatalf("checkpoint not saved: %+v", cp)
	}

	// Second run: must commit exactly records 2..5, no dupes, no skips.
	sink2 := &memSink{failAfter: -1}
	im2, err := NewImporter(testImporterCfg(sink2, store))
	if err != nil {
		t.Fatal(err)
	}
	if err := im2.Run(context.Background(), strings.NewReader(input)); err != nil {
		t.Fatal(err)
	}
	resumed := sink2.all()
	if len(resumed) != 4 {
		t.Fatalf("resume committed %d records, want 4", len(resumed))
	}
	if resumed[0].Fields[0] != "2" || resumed[3].Fields[0] != "5" {
		t.Fatalf("resume boundaries wrong: %v ... %v", resumed[0], resumed[3])
	}
	cp, _ = store.Load(context.Background())
	if cp.Records != 6 {
		t.Fatalf("final checkpoint records = %d, want 6", cp.Records)
	}
}

func TestCancelStopsAtBatchBoundary(t *testing.T) {
	store := NewMemoryCheckpointStore()
	ctx, cancel := context.WithCancel(context.Background())
	sink := SinkFunc(func(ctx context.Context, recs []Record) error {
		cancel() // cancel during first commit
		return nil
	})
	im, err := NewImporter(testImporterCfg(sink, store))
	if err != nil {
		t.Fatal(err)
	}
	err = im.Run(ctx, strings.NewReader(makeCSV(10)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	cp, _ := store.Load(context.Background())
	if cp.Records != 2 {
		t.Fatalf("checkpoint = %+v, want exactly one committed batch", cp)
	}

	// Resume with a working sink: remaining 8 records committed.
	sink2 := &memSink{failAfter: -1}
	im2, _ := NewImporter(testImporterCfg(sink2, store))
	if err := im2.Run(context.Background(), strings.NewReader(makeCSV(10))); err != nil {
		t.Fatal(err)
	}
	if got := len(sink2.all()); got != 8 {
		t.Fatalf("resume committed %d, want 8", got)
	}
	if sink2.all()[0].Fields[0] != "2" {
		t.Fatalf("resume started at wrong record: %v", sink2.all()[0])
	}
}

func TestStrictColumnCountRejects(t *testing.T) {
	sink := &memSink{failAfter: -1}
	cfg := testImporterCfg(sink, nil)
	im, err := NewImporter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = im.Run(context.Background(), strings.NewReader("id,name\n1,a\n2,b,extra\n"))
	var cc *ErrColumnCount
	if !errors.As(err, &cc) {
		t.Fatalf("want ErrColumnCount, got %v", err)
	}
	if cc.Expected != 2 || cc.Got != 3 {
		t.Fatalf("bad column error: %+v", cc)
	}
}

func TestRaggedPadPolicy(t *testing.T) {
	sink := &memSink{failAfter: -1}
	cfg := testImporterCfg(sink, nil)
	cfg.Schema.Policy = RaggedPad
	im, _ := NewImporter(cfg)
	if err := im.Run(context.Background(), strings.NewReader("id,name\n1\n2,b\n")); err != nil {
		t.Fatal(err)
	}
	recs := sink.all()
	if len(recs) != 2 || recs[0].Fields[1] != "" {
		t.Fatalf("short record not padded: %v", recs)
	}
}

func TestRaggedDropPolicy(t *testing.T) {
	sink := &memSink{failAfter: -1}
	cfg := testImporterCfg(sink, nil)
	cfg.Schema.Policy = RaggedDrop
	var reported []error
	cfg.Reader.OnError = func(err error) { reported = append(reported, err) }
	im, _ := NewImporter(cfg)
	if err := im.Run(context.Background(), strings.NewReader("id,name\n1,a,b\n2,b\n")); err != nil {
		t.Fatal(err)
	}
	recs := sink.all()
	if len(recs) != 1 || recs[0].Fields[0] != "2" {
		t.Fatalf("ragged record not dropped: %v", recs)
	}
	if len(reported) != 1 {
		t.Fatalf("drop not reported: %v", reported)
	}
	snap := im.Progress().Snapshot()
	if snap.RecordsSkipped != 1 {
		t.Fatalf("skipped not counted: %+v", snap)
	}
}

func TestHeaderMapping(t *testing.T) {
	sink := &memSink{failAfter: -1}
	cfg := testImporterCfg(sink, nil)
	im, _ := NewImporter(cfg)
	if err := im.Run(context.Background(), strings.NewReader("id,name\n1,a\n")); err != nil {
		t.Fatal(err)
	}
	// Schema is copied into the importer; verify via a fresh schema.
	var sc Schema
	if err := sc.ApplyHeader([]string{"id", "name"}); err != nil {
		t.Fatal(err)
	}
	m := sc.Map(Record{Fields: []string{"1", "a"}})
	if m["id"] != "1" || m["name"] != "a" {
		t.Fatalf("bad map: %v", m)
	}
}

func TestResumeWithBOM(t *testing.T) {
	store := NewMemoryCheckpointStore()
	input := "\xEF\xBB\xBF" + makeCSV(4)

	sink1 := &memSink{failAfter: 1}
	im1, _ := NewImporter(testImporterCfg(sink1, store))
	if err := im1.Run(context.Background(), strings.NewReader(input)); err == nil {
		t.Fatal("expected failure")
	}
	if got := len(sink1.all()); got != 2 {
		t.Fatalf("first run committed %d", got)
	}

	sink2 := &memSink{failAfter: -1}
	im2, _ := NewImporter(testImporterCfg(sink2, store))
	if err := im2.Run(context.Background(), strings.NewReader(input)); err != nil {
		t.Fatal(err)
	}
	resumed := sink2.all()
	if len(resumed) != 2 || resumed[0].Fields[0] != "2" {
		t.Fatalf("BOM resume wrong: %v", resumed)
	}
	if strings.Contains(resumed[0].Fields[0], "\ufeff") {
		t.Fatal("BOM leaked into resumed record")
	}
}

func TestProgressQueryableDuringRun(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	sink := SinkFunc(func(ctx context.Context, recs []Record) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})
	cfg := testImporterCfg(sink, nil)
	im, _ := NewImporter(cfg)
	done := make(chan error, 1)
	go func() { done <- im.Run(context.Background(), strings.NewReader(makeCSV(4))) }()
	<-started
	snap := im.Progress().Snapshot()
	if snap.RecordsRead < 2 {
		t.Fatalf("progress not live: %+v", snap)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if im.Progress().Snapshot().RecordsCommitted != 4 {
		t.Fatalf("final progress wrong: %+v", im.Progress().Snapshot())
	}
}

var _ = io.EOF
