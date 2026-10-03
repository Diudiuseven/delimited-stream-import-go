package streamimport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func parallelCfg(sink Sink, store CheckpointStore, shards int) ParallelConfig {
	return ParallelConfig{
		ImporterConfig: ImporterConfig{
			Reader: ReaderConfig{
				Dialect:        Dialect{Delimiter: ',', Quote: '"', HasHeader: true},
				MaxRecordBytes: 4096,
			},
			Schema:    Schema{Policy: RaggedReject},
			BatchSize: 3,
			Sink:      sink,
			Store:     store,
		},
		Shards: shards,
	}
}

func makeCSVData(rows int) []byte {
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&sb, "%d,name%d\n", i, i)
	}
	return []byte(sb.String())
}

func TestParallelMatchesSequentialOrder(t *testing.T) {
	data := makeCSVData(1000)

	seqSink := &memSink{failAfter: -1}
	seqImp, err := NewImporter(ImporterConfig{
		Reader: ReaderConfig{
			Dialect:        Dialect{Delimiter: ',', Quote: '"', HasHeader: true},
			MaxRecordBytes: 4096,
		},
		Schema:    Schema{Policy: RaggedReject},
		BatchSize: 3,
		Sink:      seqSink,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := seqImp.Run(context.Background(), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	parSink := &memSink{failAfter: -1}
	pi, err := NewParallelImporter(parallelCfg(parSink, nil, 8))
	if err != nil {
		t.Fatal(err)
	}
	if err := pi.Run(context.Background(), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}

	seq, par := seqSink.all(), parSink.all()
	if len(seq) != len(par) {
		t.Fatalf("sequential %d records, parallel %d", len(seq), len(par))
	}
	for i := range seq {
		if seq[i].Fields[0] != par[i].Fields[0] || seq[i].Fields[1] != par[i].Fields[1] {
			t.Fatalf("record %d differs: %v vs %v", i, seq[i].Fields, par[i].Fields)
		}
	}
	if par[0].Fields[0] != "0" || par[len(par)-1].Fields[0] != "999" {
		t.Fatalf("order broken: first=%v last=%v", par[0], par[len(par)-1])
	}
}

func TestParallelQuotedNewlines(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,note\n")
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&sb, "%d,\"line1\nline2\nline3\"\n", i)
	}
	data := []byte(sb.String())

	sink := &memSink{failAfter: -1}
	pi, err := NewParallelImporter(parallelCfg(sink, nil, 4))
	if err != nil {
		t.Fatal(err)
	}
	if err := pi.Run(context.Background(), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	recs := sink.all()
	if len(recs) != 200 {
		t.Fatalf("got %d records, want 200", len(recs))
	}
	for i, r := range recs {
		if r.Fields[0] != fmt.Sprint(i) {
			t.Fatalf("record %d out of order: %v", i, r.Fields)
		}
		if r.Fields[1] != "line1\nline2\nline3" {
			t.Fatalf("record %d field corrupted: %q", i, r.Fields[1])
		}
	}
}

func TestParallelShardFailureNoPartialCommit(t *testing.T) {
	data := makeCSVData(100)
	store := NewMemoryCheckpointStore()

	// Sink fails on the 5th commit call.
	sink := &memSink{failAfter: 4}
	pi, err := NewParallelImporter(parallelCfg(sink, store, 4))
	if err != nil {
		t.Fatal(err)
	}
	err = pi.Run(context.Background(), bytes.NewReader(data), int64(len(data)))
	if err == nil {
		t.Fatal("expected failure")
	}
	committed := sink.all()
	cp, _ := store.Load(context.Background())
	if cp.Records != int64(len(committed)) {
		t.Fatalf("checkpoint %d records but sink has %d", cp.Records, len(committed))
	}
	// Committed records must form an exact input prefix.
	for i, r := range committed {
		if r.Fields[0] != fmt.Sprint(i) {
			t.Fatalf("committed record %d is %v: not a prefix", i, r.Fields)
		}
	}

	// Resume: remaining records committed exactly once.
	sink2 := &memSink{failAfter: -1}
	pi2, err := NewParallelImporter(parallelCfg(sink2, store, 4))
	if err != nil {
		t.Fatal(err)
	}
	if err := pi2.Run(context.Background(), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	resumed := sink2.all()
	if int64(len(committed))+int64(len(resumed)) != 100 {
		t.Fatalf("total %d + %d != 100", len(committed), len(resumed))
	}
	for i, r := range resumed {
		want := fmt.Sprint(len(committed) + i)
		if r.Fields[0] != want {
			t.Fatalf("resumed record %d = %v, want id %s", i, r.Fields, want)
		}
	}
}

func TestParallelWithBOM(t *testing.T) {
	data := append([]byte{0xEF, 0xBB, 0xBF}, makeCSVData(50)...)
	sink := &memSink{failAfter: -1}
	pi, err := NewParallelImporter(parallelCfg(sink, nil, 4))
	if err != nil {
		t.Fatal(err)
	}
	if err := pi.Run(context.Background(), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	recs := sink.all()
	if len(recs) != 50 {
		t.Fatalf("got %d records", len(recs))
	}
	if strings.Contains(recs[0].Fields[0], "\ufeff") {
		t.Fatal("BOM leaked into field")
	}
}

func TestParallelProgressAndCancel(t *testing.T) {
	data := makeCSVData(500)
	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	sink := SinkFunc(func(ctx context.Context, recs []Record) error {
		once.Do(cancel)
		return nil
	})
	pi, err := NewParallelImporter(parallelCfg(sink, nil, 4))
	if err != nil {
		t.Fatal(err)
	}
	err = pi.Run(ctx, bytes.NewReader(data), int64(len(data)))
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	snap := pi.Progress().Snapshot()
	if snap.RecordsCommitted == 0 {
		t.Fatalf("expected some committed records: %+v", snap)
	}
	if snap.RecordsCommitted > 500 {
		t.Fatalf("over-committed: %+v", snap)
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParallelStrictColumnErrorAborts(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&sb, "%d,name%d\n", i, i)
	}
	sb.WriteString("broken,record,with,too,many,fields\n")
	data := []byte(sb.String())

	sink := &memSink{failAfter: -1}
	pi, err := NewParallelImporter(parallelCfg(sink, nil, 4))
	if err != nil {
		t.Fatal(err)
	}
	err = pi.Run(context.Background(), bytes.NewReader(data), int64(len(data)))
	var cc *ErrColumnCount
	if !errors.As(err, &cc) {
		t.Fatalf("want ErrColumnCount, got %v", err)
	}
	// Everything committed must be a valid prefix.
	for i, r := range sink.all() {
		if r.Fields[0] != fmt.Sprint(i) {
			t.Fatalf("committed record %d = %v: not a prefix", i, r.Fields)
		}
	}
}
