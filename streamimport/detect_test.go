package streamimport

import (
	"io"
	"strings"
	"testing"
)

func TestSampledDialect(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for i := 0; i < 1000; i++ {
		sb.WriteString("1,a\n")
	}
	input := sb.String()

	d, r, err := SampledDialect(strings.NewReader(input), 0)
	if err != nil {
		t.Fatal(err)
	}
	if d.Delimiter != ',' || !d.HasHeader {
		t.Fatalf("bad detection: %+v", d)
	}
	// The replayed reader must yield the complete original input.
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(rest) != input {
		t.Fatalf("replayed input differs: %d vs %d bytes", len(rest), len(input))
	}
}

func TestSampledDialectAmbiguousPropagates(t *testing.T) {
	_, _, err := SampledDialect(strings.NewReader("plain text\nno delimiters\n"), 0)
	if err == nil {
		t.Fatal("expected ambiguity error to propagate")
	}
}
