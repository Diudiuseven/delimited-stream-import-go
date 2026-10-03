package streamimport

import (
	"errors"
	"testing"
)

func TestDetectCSV(t *testing.T) {
	d, err := DetectDialect([]byte("id,name,age\n1,alice,30\n2,bob,40\n"))
	if err != nil {
		t.Fatal(err)
	}
	if d.Delimiter != ',' {
		t.Fatalf("delimiter = %q", d.Delimiter)
	}
	if d.LineEnding != LineEndingLF {
		t.Fatalf("line ending = %v", d.LineEnding)
	}
	if !d.HasHeader {
		t.Fatal("header not detected")
	}
}

func TestDetectTSVWithCRLF(t *testing.T) {
	d, err := DetectDialect([]byte("a\tb\r\n1\t2\r\n3\t4\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if d.Delimiter != '\t' {
		t.Fatalf("delimiter = %q", d.Delimiter)
	}
	if d.LineEnding != LineEndingCRLF {
		t.Fatalf("line ending = %v", d.LineEnding)
	}
}

func TestDetectSemicolonAndQuote(t *testing.T) {
	d, err := DetectDialect([]byte("a;b\n\"x;y\";2\n\"p;q\";3\n"))
	if err != nil {
		t.Fatal(err)
	}
	if d.Delimiter != ';' {
		t.Fatalf("delimiter = %q", d.Delimiter)
	}
	if d.Quote != '"' {
		t.Fatalf("quote = %q", d.Quote)
	}
}

func TestDetectDeterministic(t *testing.T) {
	sample := []byte("id|name|note\n1|a|\"x|y\"\n2|b|z\n3|c|w\n")
	first, err := DetectDialect(sample)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		d, err := DetectDialect(sample)
		if err != nil {
			t.Fatal(err)
		}
		if d != first {
			t.Fatalf("non-deterministic: %+v vs %+v", first, d)
		}
	}
	if first.Delimiter != '|' || first.Quote != '"' {
		t.Fatalf("bad detection: %+v", first)
	}
}

func TestDetectAmbiguous(t *testing.T) {
	// No delimiter candidate yields a consistent column count.
	_, err := DetectDialect([]byte("hello world\njust text here\n"))
	var amb *ErrAmbiguousDialect
	if !errors.As(err, &amb) {
		t.Fatalf("want ErrAmbiguousDialect, got %v", err)
	}
}

func TestDetectEmpty(t *testing.T) {
	_, err := DetectDialect(nil)
	var amb *ErrAmbiguousDialect
	if !errors.As(err, &amb) {
		t.Fatalf("want ErrAmbiguousDialect, got %v", err)
	}
}

func TestDetectIgnoresBOM(t *testing.T) {
	d, err := DetectDialect([]byte("\xEF\xBB\xBFa,b\n1,2\n"))
	if err != nil {
		t.Fatal(err)
	}
	if d.Delimiter != ',' {
		t.Fatalf("delimiter = %q", d.Delimiter)
	}
}

func TestOverride(t *testing.T) {
	d, err := DetectDialect([]byte("a,b\n1,2\n"))
	if err != nil {
		t.Fatal(err)
	}
	semi := byte(';')
	noHeader := false
	d2 := d.Apply(DialectOverride{Delimiter: &semi, HasHeader: &noHeader})
	if d2.Delimiter != ';' || d2.HasHeader {
		t.Fatalf("override not applied: %+v", d2)
	}
	// Original untouched.
	if d.Delimiter != ',' {
		t.Fatalf("original mutated: %+v", d)
	}
}

func TestDetectMixedEndings(t *testing.T) {
	d, err := DetectDialect([]byte("a,b\n1,2\r\n3,4\n5,6\r\n7,8\n"))
	if err != nil {
		t.Fatal(err)
	}
	if d.LineEnding != LineEndingMixed {
		t.Fatalf("line ending = %v", d.LineEnding)
	}
}
