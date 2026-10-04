package delimited

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestStreamImporter(t *testing.T) {
	input := "a,b,c\n1,2,3\n4,5,6\n"
	sink := &collectingSink{}
	im, err := NewStreamImporter(strings.NewReader(input), Config{}, sink)
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
}

func TestStreamImporterCancel(t *testing.T) {
	input := "a,b\n" + strings.Repeat("x,y\n", 10000)
	sink := &collectingSink{}
	im, err := NewStreamImporter(strings.NewReader(input), Config{BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = im.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestImporterBOM(t *testing.T) {
	input := "\xEF\xBB\xBFa,b\n1,2\n3,4\n"
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
	if recs[0].Fields[0] != "1" {
		t.Fatalf("BOM leaked into data: %q", recs[0].Fields[0])
	}
}

func TestImporterExplicitFormat(t *testing.T) {
	// Semicolon-delimited with explicit format.
	input := "a;b;c\n1;2;3\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	f := Format{Delimiter: ';', Quote: '"', Escape: EscapeDoubleQuote, LineEnding: LineEndingAuto, Confidence: 1}
	im, err := NewImporter(src, Config{Format: &f}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 1 || recs[0].Fields[0] != "1" {
		t.Fatalf("got %v", recs)
	}
}

func TestImporterUncertainRequiresOverride(t *testing.T) {
	// Single-column input: detection is uncertain.
	input := "a\nb\nc\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	err = im.Run(context.Background())
	var ue *ErrUncertainFormat
	if !errors.As(err, &ue) {
		t.Fatalf("got %v, want ErrUncertainFormat", err)
	}
	// With explicit format it works.
	f := DefaultFormat()
	im2, err := NewImporter(NewBytesSource([]byte(input)), Config{Format: &f, NoHeaderRow: true}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestImporterEmptyInput(t *testing.T) {
	src := NewBytesSource(nil)
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{NoHeaderRow: true}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	recs := sink.snapshot()
	if len(recs) != 0 {
		t.Fatalf("got %d records, want 0", len(recs))
	}
}

func TestImporterOnErrorCallback(t *testing.T) {
	input := "a,b\n\"bad\n1,2\n"
	src := NewBytesSource([]byte(input))
	var errCount int
	sink := &collectingSink{}
	cfg := Config{
		NoHeaderRow: true,
		Strict:      false,
		OnError:     func(error) { errCount++ },
	}
	im, err := NewImporter(src, cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if errCount < 1 {
		t.Fatalf("OnError called %d times, want >= 1", errCount)
	}
	stats := im.Stats()
	if stats.Errors < 1 {
		t.Fatalf("errors = %d, want >= 1", stats.Errors)
	}
}

func TestImporterHeaderMapping(t *testing.T) {
	input := "name,age,city\nalice,30,NYC\nbob,25,LA\n"
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
	schema := im.Schema()
	if schema == nil {
		t.Fatal("schema is nil")
	}
	idx, ok := schema.Index("city")
	if !ok || idx != 2 {
		t.Fatalf("city index = %d, ok=%v", idx, ok)
	}
	if got, _ := recs[0].Get(schema, "city"); got != "NYC" {
		t.Fatalf("city = %q, want NYC", got)
	}
	if got, _ := recs[0].Get(schema, "name"); got != "alice" {
		t.Fatalf("name = %q, want alice", got)
	}
}

func TestSchemaDuplicateHeader(t *testing.T) {
	_, err := NewSchema([]string{"a", "b", "a"})
	var dh *ErrDuplicateHeader
	if !errors.As(err, &dh) {
		t.Fatalf("got %v, want ErrDuplicateHeader", err)
	}
}

func TestParserEmptyInput(t *testing.T) {
	p := NewParser(strings.NewReader(""), DefaultFormat(), 0)
	_, err := p.Next()
	if err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestParserNoQuote(t *testing.T) {
	f := DefaultFormat()
	f.Quote = 0
	input := "a,b,c\n1,2,3\n"
	recs, err := parseAll(t, input, f)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
}

func TestImporterMixedNewlines(t *testing.T) {
	input := "a,b\r\nc,d\re,f\ng,h"
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
	if len(recs) != 4 {
		t.Fatalf("got %d records, want 4", len(recs))
	}
}

func TestImporterStats(t *testing.T) {
	input := "a,b\n\n1,2\n\n3,4\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{NoHeaderRow: true}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats := im.Stats()
	if stats.RecordsCommitted != 3 {
		t.Fatalf("committed = %d, want 3", stats.RecordsCommitted)
	}
	if stats.BlankLines != 2 {
		t.Fatalf("blank lines = %d, want 2", stats.BlankLines)
	}
	if stats.BytesRead != int64(len(input)) {
		t.Fatalf("bytes read = %d, want %d", stats.BytesRead, len(input))
	}
}

func TestImporterProgressQueryable(t *testing.T) {
	// Stats must be queryable while the import is running.
	var sb strings.Builder
	sb.WriteString("a,b\n")
	for i := 0; i < 1000; i++ {
		sb.WriteString(fmt.Sprintf("%d,%d\n", i, i))
	}
	src := NewBytesSource([]byte(sb.String()))
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{ShardCount: 4, BatchSize: 10}, sink)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- im.Run(context.Background())
	}()
	// Poll stats while running.
	for i := 0; i < 100; i++ {
		s := im.Stats()
		if s.RecordsCommitted > 0 {
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestParserEmptyQuotedFieldAtEOF(t *testing.T) {
	input := `a,b,""`
	recs, err := parseAll(t, input, DefaultFormat())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	if !equalStrings(recs[0].Fields, []string{"a", "b", ""}) {
		t.Fatalf("fields = %v", recs[0].Fields)
	}
}

func TestParserQuotedDelimiterAtEOF(t *testing.T) {
	input := `a,"b,c"`
	recs, err := parseAll(t, input, DefaultFormat())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	if !equalStrings(recs[0].Fields, []string{"a", "b,c"}) {
		t.Fatalf("fields = %v", recs[0].Fields)
	}
}

func TestImporterShardCountOne(t *testing.T) {
	input := "a,b\n1,2\n3,4\n"
	src := NewBytesSource([]byte(input))
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{ShardCount: 1}, sink)
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

func TestImporterExplicitSchema(t *testing.T) {
	// Provide an explicit schema; the first record is data, not a header.
	input := "1,2,3\n4,5,6\n"
	src := NewBytesSource([]byte(input))
	schema, err := NewSchema([]string{"x", "y", "z"})
	if err != nil {
		t.Fatal(err)
	}
	sink := &collectingSink{}
	im, err := NewImporter(src, Config{Schema: schema}, sink)
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
	if got, _ := recs[0].Get(schema, "z"); got != "3" {
		t.Fatalf("z = %q, want 3", got)
	}
}
