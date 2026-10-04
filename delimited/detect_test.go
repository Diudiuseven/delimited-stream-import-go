package delimited

import (
	"io"
	"strings"
	"testing"
)

func TestDetectDelimiters(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  rune
	}{
		{"comma", "a,b,c\n1,2,3\n4,5,6\n", ','},
		{"tab", "a\tb\tc\n1\t2\t3\n", '\t'},
		{"semicolon", "a;b;c\n1;2;3\n", ';'},
		{"pipe", "a|b|c\n1|2|3\n", '|'},
		{"colon", "a:b:c\n1:2:3\n", ':'},
		{"comma with quoted commas", `a,b,"c,d"` + "\n" + `1,2,"3,4"` + "\n", ','},
		{"semicolon with quoted semicolons", `a;b;"c;d"` + "\n" + `1;2;"3;4"` + "\n", ';'},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := DetectFormatBytes([]byte(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if f.Delimiter != tc.want {
				t.Fatalf("delimiter = %q, want %q (uncertain=%v reasons=%v)", f.Delimiter, tc.want, f.Uncertain, f.Reasons)
			}
			if f.Uncertain {
				t.Fatalf("unexpected uncertainty: %v", f.Reasons)
			}
		})
	}
}

func TestDetectQuote(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  rune
	}{
		{"double quote", `"a","b"` + "\n" + `"c","d"` + "\n", '"'},
		{"single quote", `'a','b'` + "\n" + `'c','d'` + "\n", '\''},
		{"no quote", "a,b,c\n1,2,3\n", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := DetectFormatBytes([]byte(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if f.Quote != tc.want {
				t.Fatalf("quote = %q, want %q", f.Quote, tc.want)
			}
		})
	}
}

func TestDetectEscape(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  EscapeStyle
	}{
		{"doubling", `"a""b","c"` + "\n" + `"x""y","z"` + "\n", EscapeDoubleQuote},
		{"backslash", `"a\"b","c"` + "\n" + `"x\"y","z"` + "\n", EscapeBackslash},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := DetectFormatBytes([]byte(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if f.Escape != tc.want {
				t.Fatalf("escape = %v, want %v", f.Escape, tc.want)
			}
		})
	}
}

func TestDetectLineEnding(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  LineEnding
	}{
		{"lf", "a,b\nc,d\n", LineEndingLF},
		{"crlf", "a,b\r\nc,d\r\n", LineEndingCRLF},
		{"cr", "a,b\rc,d\r", LineEndingCR},
		{"mixed prefers crlf", "a,b\r\nc,d\ne,f\r\n", LineEndingCRLF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := DetectFormatBytes([]byte(tc.input))
			if err != nil {
				t.Fatal(err)
			}
			if f.LineEnding != tc.want {
				t.Fatalf("line ending = %v, want %v", f.LineEnding, tc.want)
			}
		})
	}
}

func TestDetectBOM(t *testing.T) {
	input := "\xEF\xBB\xBFa,b\nc,d\n"
	f, err := DetectFormatBytes([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if f.Delimiter != ',' {
		t.Fatalf("delimiter = %q, want ,", f.Delimiter)
	}
}

func TestDetectUncertain(t *testing.T) {
	// Single-column records: delimiter unclear.
	f, err := DetectFormatBytes([]byte("a\nb\nc\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !f.Uncertain {
		t.Fatalf("expected uncertainty, got confident format: %v", f)
	}
	if len(f.Reasons) == 0 {
		t.Fatal("expected reasons for uncertainty")
	}
}

func TestDetectEmpty(t *testing.T) {
	f, err := DetectFormatBytes(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Uncertain {
		t.Fatal("expected uncertainty for empty input")
	}
}

func TestDetectDeterministic(t *testing.T) {
	input := `"a","b","c"` + "\n" + `1,2,3` + "\n" + `"x,y",z,1` + "\n"
	r1 := strings.NewReader(input)
	r2 := strings.NewReader(input)
	f1, err := DetectFormat(r1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := DetectFormat(r2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if f1.Delimiter != f2.Delimiter || f1.Quote != f2.Quote || f1.Escape != f2.Escape || f1.LineEnding != f2.LineEnding {
		t.Fatalf("non-deterministic detection: %v vs %v", f1, f2)
	}
	// Reader must be reset to start.
	pos, _ := r1.Seek(0, io.SeekCurrent)
	if pos != 0 {
		t.Fatalf("reader not reset, position = %d", pos)
	}
}

func TestDetectStableAcrossSamples(t *testing.T) {
	// Same dialect, different content: detection must be stable.
	inputs := []string{
		"a,b,c\n1,2,3\n4,5,6\n",
		"x,y,z\n7,8,9\n0,1,2\n",
		"foo,bar,baz\nq,w,e\n",
	}
	var d rune
	for i, in := range inputs {
		f, err := DetectFormatBytes([]byte(in))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			d = f.Delimiter
			continue
		}
		if f.Delimiter != d {
			t.Fatalf("sample %d: delimiter changed from %q to %q", i, d, f.Delimiter)
		}
	}
}
