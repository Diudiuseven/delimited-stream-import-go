package streamimport

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// TestParallelSequentialEquivalence generates a pseudo-random CSV with
// quoted fields, embedded delimiters/newlines and ragged-free rows, then
// checks that every shard count produces exactly the sequential output.
func TestParallelSequentialEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	var sb strings.Builder
	sb.WriteString("id,txt,val\n")
	rows := 3000
	for i := 0; i < rows; i++ {
		switch rng.Intn(6) {
		case 0:
			fmt.Fprintf(&sb, "%d,\"quoted, with delim\",%d\n", i, rng.Intn(100))
		case 1:
			fmt.Fprintf(&sb, "%d,\"multi\nline\",%d\n", i, rng.Intn(100))
		case 2:
			fmt.Fprintf(&sb, "%d,\"escaped \"\"quote\"\"\",%d\n", i, rng.Intn(100))
		case 3:
			fmt.Fprintf(&sb, "%d,plain,%d\n", i, rng.Intn(100))
		case 4:
			fmt.Fprintf(&sb, "%d,\"mix,\"\"q\"\"\nend\",%d\n", i, rng.Intn(100))
		default:
			fmt.Fprintf(&sb, "%d,x,%d\n", i, rng.Intn(100))
		}
	}
	data := []byte(sb.String())

	baseCfg := func(sink Sink) ImporterConfig {
		return ImporterConfig{
			Reader: ReaderConfig{
				Dialect:        Dialect{Delimiter: ',', Quote: '"', HasHeader: true},
				MaxRecordBytes: 1 << 16,
			},
			Schema:    Schema{Policy: RaggedReject},
			BatchSize: 7,
			Sink:      sink,
		}
	}

	seqSink := &memSink{failAfter: -1}
	seq, err := NewImporter(baseCfg(seqSink))
	if err != nil {
		t.Fatal(err)
	}
	if err := seq.Run(context.Background(), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	want := seqSink.all()

	for _, shards := range []int{1, 2, 3, 5, 8, 16} {
		sink := &memSink{failAfter: -1}
		pi, err := NewParallelImporter(ParallelConfig{ImporterConfig: baseCfg(sink), Shards: shards})
		if err != nil {
			t.Fatal(err)
		}
		if err := pi.Run(context.Background(), bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("shards=%d: %v", shards, err)
		}
		got := sink.all()
		if len(got) != len(want) {
			t.Fatalf("shards=%d: got %d records, want %d", shards, len(got), len(want))
		}
		for i := range want {
			if len(got[i].Fields) != 3 || got[i].Fields[0] != want[i].Fields[0] ||
				got[i].Fields[1] != want[i].Fields[1] || got[i].Fields[2] != want[i].Fields[2] {
				t.Fatalf("shards=%d record %d differs:\n got %q\nwant %q", shards, i, got[i].Fields, want[i].Fields)
			}
		}
	}
}
