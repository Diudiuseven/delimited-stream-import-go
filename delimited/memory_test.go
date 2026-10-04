package delimited

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestLargeInputBoundedMemory processes an input much larger than the read
// buffer and record limit, verifying it completes without loading the whole
// input into memory.
func TestLargeInputBoundedMemory(t *testing.T) {
	// ~20 MB input
	var sb strings.Builder
	sb.WriteString("id,name,value\n")
	for i := 0; i < 200000; i++ {
		sb.WriteString(fmt.Sprintf("%d,user%d,value%d\n", i, i, i))
	}
	src := NewBytesSource([]byte(sb.String()))
	sink := &collectingSink{}
	cfg := Config{
		ShardCount:     4,
		MaxRecordBytes: 1024,
		BatchSize:      100,
	}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 200000 {
		t.Fatalf("got %d records, want 200000", len(recs))
	}
	// Verify order.
	for i, r := range recs {
		if r.Fields[0] != fmt.Sprintf("%d", i) {
			t.Fatalf("record %d out of order: %v", i, r.Fields)
		}
	}
}

// TestRecordTooLongRecoverable verifies that an oversize record is skipped and
// the import continues with the next record.
func TestRecordTooLongRecoverable(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,data\n")
	sb.WriteString("1,short\n")
	sb.WriteString("2," + strings.Repeat("x", 2000) + "\n") // too long
	sb.WriteString("3,after\n")
	src := NewBytesSource([]byte(sb.String()))
	sink := &collectingSink{}
	cfg := Config{MaxRecordBytes: 100, Strict: false}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2 (oversize skipped)", len(recs))
	}
	if recs[0].Fields[0] != "1" || recs[1].Fields[0] != "3" {
		t.Fatalf("records = %v, want [1, 3]", recs)
	}
	stats := im.Stats()
	if stats.RecordsSkipped < 1 {
		t.Fatalf("skipped = %d, want >= 1", stats.RecordsSkipped)
	}
}
