package delimited

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func parseAll(t *testing.T, input string, f Format) ([]Record, error) {
	t.Helper()
	p := NewParser(strings.NewReader(input), f, 0)
	var recs []Record
	for {
		rec, err := p.Next()
		if err == io.EOF {
			return recs, nil
		}
		if err != nil {
			return recs, err
		}
		recs = append(recs, rec)
	}
}

func TestParserBasic(t *testing.T) {
	input := "a,b,c\n1,2,3\n"
	recs, err := parseAll(t, input, DefaultFormat())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if got := recs[0].Fields; len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("record 0 = %v", got)
	}
	if got := recs[1].Fields; len(got) != 3 || got[0] != "1" || got[1] != "2" || got[2] != "3" {
		t.Fatalf("record 1 = %v", got)
	}
}

func TestParserQuotedFields(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  []string
	}{
		{"simple quoted", `"a","b","c"` + "\n", []string{"a", "b", "c"}},
		{"delimiter inside", `"a,b","c,d"` + "\n", []string{"a,b", "c,d"}},
		{"newline inside", "\"a\nb\",\"c\"\n", []string{"a\nb", "c"}},
		{"crlf inside", "\"a\r\nb\",\"c\"\r\n", []string{"a\r\nb", "c"}},
		{"escaped quote", `"a""b","c"` + "\n", []string{"a\"b", "c"}},
		{"escaped quote at edges", `"""a""","x"` + "\n", []string{"\"a\"", "x"}},
		{"spaces after quote", `"a" , "b"` + "\n", []string{"a", "b"}},
		{"empty quoted", `"",""` + "\n", []string{"", ""}},
		{"trailing empty field", "a,b,\n", []string{"a", "b", ""}},
		{"only delimiters", ",,\n", []string{"", "", ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recs, err := parseAll(t, tc.input, DefaultFormat())
			if err != nil {
				t.Fatal(err)
			}
			if len(recs) != 1 {
				t.Fatalf("got %d records, want 1", len(recs))
			}
			if got := recs[0].Fields; !equalStrings(got, tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParserMixedNewlines(t *testing.T) {
	input := "a,b\r\nc,d\re,f\ng,h"
	recs, err := parseAll(t, input, DefaultFormat())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 4 {
		t.Fatalf("got %d records, want 4", len(recs))
	}
	want := [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}}
	for i := range want {
		if !equalStrings(recs[i].Fields, want[i]) {
			t.Fatalf("record %d = %v, want %v", i, recs[i].Fields, want[i])
		}
	}
}

func TestParserBOM(t *testing.T) {
	input := "\xEF\xBB\xBFa,b\nc,d\n"
	recs, err := parseAll(t, input, DefaultFormat())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	if recs[0].Fields[0] != "a" {
		t.Fatalf("BOM leaked into field: %q", recs[0].Fields[0])
	}
	if recs[0].Pos.Offset != 3 {
		t.Fatalf("record 0 offset = %d, want 3", recs[0].Pos.Offset)
	}
}

func TestParserBOMRejected(t *testing.T) {
	for _, enc := range []string{"\xFF\xFE", "\xFE\xFF"} {
		p := NewParser(strings.NewReader(enc+"a,b\n"), DefaultFormat(), 0)
		_, err := p.Next()
		var ue *ErrUnsupportedEncoding
		if !errors.As(err, &ue) {
			t.Fatalf("got %v, want ErrUnsupportedEncoding", err)
		}
	}
}

func TestParserMalformedQuote(t *testing.T) {
	input := `a"b,c` + "\n" + `d,e` + "\n"
	p := NewParser(strings.NewReader(input), DefaultFormat(), 0)
	_, err := p.Next()
	var mq *ErrMalformedQuote
	if !errors.As(err, &mq) {
		t.Fatalf("got %v, want ErrMalformedQuote", err)
	}
	if mq.Pos().Line != 1 {
		t.Fatalf("error line = %d, want 1", mq.Pos().Line)
	}
	// parser must recover and continue
	rec, err := p.Next()
	if err != nil {
		t.Fatalf("after error: %v", err)
	}
	if !equalStrings(rec.Fields, []string{"d", "e"}) {
		t.Fatalf("recovered record = %v", rec.Fields)
	}
}

func TestParserUnclosedQuote(t *testing.T) {
	input := `"a,b`
	p := NewParser(strings.NewReader(input), DefaultFormat(), 0)
	_, err := p.Next()
	var uq *ErrUnclosedQuote
	if !errors.As(err, &uq) {
		t.Fatalf("got %v, want ErrUnclosedQuote", err)
	}
	if uq.Pos().Offset != 0 {
		t.Fatalf("error offset = %d, want 0", uq.Pos().Offset)
	}
	_, err = p.Next()
	if err != io.EOF {
		t.Fatalf("after unclosed quote, got %v, want io.EOF", err)
	}
}

func TestParserRecordTooLong(t *testing.T) {
	input := "aaaaa,bbbbb\ncc,dd\n"
	p := NewParser(strings.NewReader(input), DefaultFormat(), 8)
	_, err := p.Next()
	var rtl *ErrRecordTooLong
	if !errors.As(err, &rtl) {
		t.Fatalf("got %v, want ErrRecordTooLong", err)
	}
	if rtl.Limit != 8 {
		t.Fatalf("limit = %d, want 8", rtl.Limit)
	}
	rec, err := p.Next()
	if err != nil {
		t.Fatalf("after too-long: %v", err)
	}
	if !equalStrings(rec.Fields, []string{"cc", "dd"}) {
		t.Fatalf("recovered record = %v", rec.Fields)
	}
}

func TestParserBlankLines(t *testing.T) {
	input := "a,b\n\nc,d\n\n"
	recs, err := parseAll(t, input, DefaultFormat())
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
}

func TestParserBackslashEscape(t *testing.T) {
	f := DefaultFormat()
	f.Escape = EscapeBackslash
	cases := []struct {
		input string
		want  []string
	}{
		{`"a\"b","c"` + "\n", []string{`a"b`, "c"}},
		{`"a\\b","c"` + "\n", []string{`a\b`, "c"}},
		{`a\"b,c` + "\n", []string{`a"b`, "c"}},
	}
	for _, tc := range cases {
		recs, err := parseAll(t, tc.input, f)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 1 || !equalStrings(recs[0].Fields, tc.want) {
			t.Fatalf("got %v, want %v", recs, tc.want)
		}
	}
}

func TestParserPositions(t *testing.T) {
	input := "ab,cd\nef,gh\n"
	recs, err := parseAll(t, input, DefaultFormat())
	if err != nil {
		t.Fatal(err)
	}
	if recs[0].Pos.Offset != 0 || recs[0].Pos.Line != 1 {
		t.Fatalf("record0 pos = %v", recs[0].Pos)
	}
	if recs[1].Pos.Offset != 6 || recs[1].Pos.Line != 2 {
		t.Fatalf("record1 pos = %v, want offset 6 line 2", recs[1].Pos)
	}
	if recs[1].EndPos.Offset != 12 {
		t.Fatalf("record1 end = %v, want offset 12", recs[1].EndPos)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
