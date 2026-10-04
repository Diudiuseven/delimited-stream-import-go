package delimited

import (
	"bufio"
	"io"
	"os"
)

// Source is a seekable, sized input for sharded and resumable imports.
// *os.File satisfies it directly.
type Source interface {
	io.ReaderAt
	// Size returns the total number of bytes in the source.
	Size() int64
}

// FileSource wraps an *os.File as a Source.
type FileSource struct {
	file *os.File
	size int64
}

// OpenFile opens a file as a Source.
func OpenFile(path string) (*FileSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close() //nolint:errcheck
		return nil, err
	}
	return &FileSource{file: f, size: fi.Size()}, nil
}

// ReadAt implements io.ReaderAt.
func (s *FileSource) ReadAt(p []byte, off int64) (int, error) {
	return s.file.ReadAt(p, off)
}

// Size returns the file size.
func (s *FileSource) Size() int64 { return s.size }

// Close closes the underlying file.
func (s *FileSource) Close() error { return s.file.Close() }

// BytesSource is an in-memory Source for tests and small inputs.
type BytesSource struct {
	data []byte
}

// NewBytesSource returns a Source backed by data.
func NewBytesSource(data []byte) *BytesSource {
	return &BytesSource{data: data}
}

// ReadAt implements io.ReaderAt.
func (s *BytesSource) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(p, s.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Size returns the data length.
func (s *BytesSource) Size() int64 { return int64(len(s.data)) }

// shardBoundaries computes record-aligned shard boundaries over [0, size).
// Boundaries are strictly increasing record-start offsets; shard i covers
// [boundaries[i], boundaries[i+1]). The scan is a single pass over the input
// tracking quote state, so it is O(n) and robust against newlines inside
// quoted fields.
func shardBoundaries(src Source, shardCount int, f Format) ([]int64, error) {
	size := src.Size()
	if shardCount <= 1 || size == 0 {
		return []int64{0, size}, nil
	}
	targets := make([]int64, 0, shardCount-1)
	for i := 1; i < shardCount; i++ {
		targets = append(targets, int64(i)*size/int64(shardCount))
	}
	boundaries := make([]int64, 0, shardCount+1)
	boundaries = append(boundaries, 0)

	r := io.NewSectionReader(src, 0, size)
	br := bufio.NewReaderSize(r, 64*1024)

	inQuotes := false
	atFieldStart := true
	offset := int64(0)
	ti := 0

	flush := func(recordStart int64) {
		for ti < len(targets) && targets[ti] <= recordStart {
			boundaries = append(boundaries, recordStart)
			ti++
		}
	}

	for ti < len(targets) {
		b, err := br.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if inQuotes {
			if f.Escape == EscapeBackslash && b == '\\' {
				offset++
				if _, err := br.ReadByte(); err == nil {
					offset++
					// The escaped byte is either a quote/backslash or data;
					// either way we remain inside the quoted field.
				}
				continue
			}
			if f.Quote != 0 && b == byte(f.Quote) {
				offset++
				if f.Escape == EscapeDoubleQuote {
					if b2, err := br.ReadByte(); err == nil && b2 == byte(f.Quote) {
						offset++
						continue
					}
					if err == nil {
						br.UnreadByte()
					}
				}
				inQuotes = false
				continue
			}
			offset++
			continue
		}
		if f.Quote != 0 && b == byte(f.Quote) && atFieldStart {
			inQuotes = true
			atFieldStart = false
			offset++
			continue
		}
		if b == ' ' || b == '\t' {
			offset++
			continue
		}
		if isTerm(b) {
			offset++
			if b == '\r' {
				if b2, err := br.ReadByte(); err == nil && b2 == '\n' {
					offset++
				} else if err == nil {
					br.UnreadByte()
				}
			}
			atFieldStart = true
			flush(offset)
			continue
		}
		if rune(b) == f.Delimiter {
			atFieldStart = true
		} else {
			atFieldStart = false
		}
		offset++
	}
	for ti < len(targets) {
		boundaries = append(boundaries, size)
		ti++
	}
	boundaries = append(boundaries, size)
	return boundaries, nil
}
