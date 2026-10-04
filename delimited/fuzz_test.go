package delimited

import (
	"io"
	"strings"
	"testing"
)

// FuzzParser ensures the parser never panics on arbitrary input and always
// makes progress (terminates).
func FuzzParser(f *testing.F) {
	f.Add("a,b,c\n1,2,3\n")
	f.Add(`"a,b","c"` + "\n")
	f.Add("a,b\r\nc,d\re,f\n")
	f.Add("\xEF\xBB\xBFa,b\n")
	f.Add(`"unclosed`)
	f.Add(`a"b,c` + "\n")
	f.Add("")
	f.Add(",,,\n")
	f.Add(`"""x"""` + "\n")
	f.Fuzz(func(t *testing.T, input string) {
		p := NewParser(strings.NewReader(input), DefaultFormat(), 1024)
		for {
			_, err := p.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				// Any error is acceptable as long as the parser terminates
				// and does not panic. Fatal errors (e.g. unsupported
				// encoding) stop the stream; recoverable ones continue.
				if _, ok := err.(*ErrUnsupportedEncoding); ok {
					return
				}
				if _, ok := err.(ParseError); ok {
					continue
				}
				t.Fatalf("unexpected error type: %T %v", err, err)
			}
		}
	})
}
