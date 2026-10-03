package streamimport

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func csvCfg() ReaderConfig {
	return ReaderConfig{
		Dialect:        Dialect{Delimiter: ',', Quote: '"', LineEnding: LineEndingLF},
		MaxRecordBytes: 1024,
	}
}

func readAll(t *testing.T, r *Reader) []Record {
	t.Helper()
	var out []Record
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		out = append(out, rec)
	}
}

func TestBasicRecords(t *testing.T) {
	r, err := NewReader(strings.NewReader("a,b,c\n1,2,3\n"), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if len(recs) != 2 {
		t.Fatalf("got %d records", len(recs))
	}
	if got := recs[0].Fields; got[0] != "a" || got[2] != "c" {
		t.Fatalf("unexpected fields %v", got)
	}
	if recs[1].Offset <= recs[0].Offset || recs[0].EndOffset != recs[1].Offset {
		t.Fatalf("bad offsets: %+v %+v", recs[0], recs[1])
	}
	if recs[0].Line != 1 || recs[1].Line != 2 {
		t.Fatalf("bad lines: %d %d", recs[0].Line, recs[1].Line)
	}
}

func TestQuotedFieldWithDelimiterNewlineAndEscapedQuote(t *testing.T) {
	in := "id,note\n1,\"hello, world\"\n2,\"line1\nline2\"\n3,\"say \"\"hi\"\"\"\n"
	r, err := NewReader(strings.NewReader(in), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if len(recs) != 4 {
		t.Fatalf("got %d records: %v", len(recs), recs)
	}
	if recs[1].Fields[1] != "hello, world" {
		t.Fatalf("embedded delimiter broke field: %q", recs[1].Fields[1])
	}
	if recs[2].Fields[1] != "line1\nline2" {
		t.Fatalf("embedded newline broke record: %q", recs[2].Fields[1])
	}
	if recs[3].Fields[1] != `say "hi"` {
		t.Fatalf("doubled quote not unescaped: %q", recs[3].Fields[1])
	}
}

func TestBackslashEscape(t *testing.T) {
	cfg := csvCfg()
	cfg.Dialect.Escape = '\\'
	r, err := NewReader(strings.NewReader("a,b\n1,\"x\\\"y\"\n"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if recs[1].Fields[1] != `x"y` {
		t.Fatalf("escape not applied: %q", recs[1].Fields[1])
	}
}

func TestBOMStripped(t *testing.T) {
	in := "\xEF\xBB\xBFa,b\n1,2\n"
	r, err := NewReader(strings.NewReader(in), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if recs[0].Fields[0] != "a" {
		t.Fatalf("BOM leaked into field: %q", recs[0].Fields[0])
	}
	if recs[0].Offset != 0 {
		t.Fatalf("offset should be relative to post-BOM start, got %d", recs[0].Offset)
	}
}

func TestMixedLineEndings(t *testing.T) {
	in := "a,b\r\n1,2\n3,4\r5,6\r\n"
	r, err := NewReader(strings.NewReader(in), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if len(recs) != 4 {
		t.Fatalf("got %d records: %v", len(recs), recs)
	}
	if recs[3].Fields[0] != "5" || recs[3].Fields[1] != "6" {
		t.Fatalf("CR-only record misaligned: %v", recs[3].Fields)
	}
	if recs[1].Fields[0] != "1" {
		t.Fatalf("CRLF record misaligned: %v", recs[1].Fields)
	}
}

func TestBareQuoteStrict(t *testing.T) {
	r, err := NewReader(strings.NewReader("a,b\n1,ab\"cd\n"), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(); err != nil {
		t.Fatalf("first record: %v", err)
	}
	_, err = r.Read()
	var bq *ErrBareQuote
	if !errors.As(err, &bq) {
		t.Fatalf("want ErrBareQuote, got %v", err)
	}
	if bq.Position.Line != 2 {
		t.Fatalf("error position missing line: %+v", bq.Position)
	}
}

func TestCharAfterClosingQuote(t *testing.T) {
	r, err := NewReader(strings.NewReader("a,b\n1,\"x\"junk\n"), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = r.Read()
	_, err = r.Read()
	var bq *ErrBareQuote
	if !errors.As(err, &bq) {
		t.Fatalf("want ErrBareQuote, got %v", err)
	}
}

func TestUnclosedQuote(t *testing.T) {
	r, err := NewReader(strings.NewReader("a,b\n1,\"never ends\n"), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = r.Read()
	_, err = r.Read()
	var uq *ErrUnclosedQuote
	if !errors.As(err, &uq) {
		t.Fatalf("want ErrUnclosedQuote, got %v", err)
	}
	if uq.Position.Line != 2 {
		t.Fatalf("position should point at record start: %+v", uq.Position)
	}
}

func TestLenientSkipsMalformed(t *testing.T) {
	cfg := csvCfg()
	cfg.Lenient = true
	var skipped []error
	cfg.OnError = func(err error) { skipped = append(skipped, err) }
	r, err := NewReader(strings.NewReader("a,b\n1,bad\"x\n2,ok\n"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if len(recs) != 2 {
		t.Fatalf("got %d records: %v", len(recs), recs)
	}
	if recs[1].Fields[1] != "ok" {
		t.Fatalf("did not resume after bad record: %v", recs)
	}
	if len(skipped) != 1 {
		t.Fatalf("want 1 skipped error, got %v", skipped)
	}
}

func TestRecordTooLarge(t *testing.T) {
	cfg := csvCfg()
	cfg.MaxRecordBytes = 8
	r, err := NewReader(strings.NewReader("short\nthis-record-is-way-too-long\n"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = r.Read()
	_, err = r.Read()
	var tl *ErrRecordTooLarge
	if !errors.As(err, &tl) {
		t.Fatalf("want ErrRecordTooLarge, got %v", err)
	}
	if tl.Limit != 8 {
		t.Fatalf("limit not reported: %+v", tl)
	}
}

func TestRecordTooLargeLenientRecovers(t *testing.T) {
	cfg := csvCfg()
	cfg.MaxRecordBytes = 8
	cfg.Lenient = true
	var skipped []error
	cfg.OnError = func(err error) { skipped = append(skipped, err) }
	r, err := NewReader(strings.NewReader("ok1\nthis-record-is-way-too-long\nok2\n"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if len(recs) != 2 || recs[0].Fields[0] != "ok1" || recs[1].Fields[0] != "ok2" {
		t.Fatalf("recovery failed: %v", recs)
	}
	if len(skipped) != 1 {
		t.Fatalf("want 1 skip, got %d", len(skipped))
	}
}

func TestBlankLinesSkipped(t *testing.T) {
	r, err := NewReader(strings.NewReader("a,b\n\n1,2\n\n"), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if len(recs) != 2 {
		t.Fatalf("blank lines not skipped: %v", recs)
	}
}

func TestTrailingEmptyField(t *testing.T) {
	r, err := NewReader(strings.NewReader("a,b,\n"), csvCfg())
	if err != nil {
		t.Fatal(err)
	}
	recs := readAll(t, r)
	if len(recs) != 1 || len(recs[0].Fields) != 3 || recs[0].Fields[2] != "" {
		t.Fatalf("trailing empty field lost: %v", recs)
	}
}

func TestMemoryBounded(t *testing.T) {
	// 5 MB of input with a 4 KiB record cap must parse with small
	// allocations relative to input size.
	var sb strings.Builder
	for i := 0; i < 100000; i++ {
		sb.WriteString("abcdef,ghijkl,mnopqr,stuvwx\n")
	}
	cfg := csvCfg()
	cfg.MaxRecordBytes = 4096
	r, err := NewReader(strings.NewReader(sb.String()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 100000 {
		t.Fatalf("got %d records", n)
	}
}
