package streamimport

import (
	"bytes"
	"io"
)

// DefaultSampleSize is the number of bytes SampledDialect reads for
// detection when sampleSize <= 0.
const DefaultSampleSize = 64 * 1024

// SampledDialect reads up to sampleSize bytes from r, detects the
// dialect from the sample, and returns the dialect together with a
// reader that replays the sample followed by the rest of r, so no
// input is lost. Detection is deterministic; when it fails with
// *ErrAmbiguousDialect the caller must supply a DialectOverride and
// call DetectDialect/Apply itself — the library never silently
// guesses.
func SampledDialect(r io.Reader, sampleSize int) (Dialect, io.Reader, error) {
	if sampleSize <= 0 {
		sampleSize = DefaultSampleSize
	}
	buf := make([]byte, sampleSize)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return Dialect{}, nil, err
	}
	sample := buf[:n]
	d, err := DetectDialect(sample)
	if err != nil {
		return Dialect{}, nil, err
	}
	return d, io.MultiReader(bytes.NewReader(sample), r), nil
}
