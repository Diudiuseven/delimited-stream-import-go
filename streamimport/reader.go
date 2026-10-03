package streamimport

import "io"

// Record is one parsed row.
type Record struct {
	// Fields holds the decoded field values.
	Fields []string
	// Offset is the absolute byte offset where this record starts
	// in the original source (after any BOM).
	Offset int64
	// EndOffset is the byte offset one past the record terminator;
	// resuming a reader at EndOffset continues with the next record.
	EndOffset int64
	// Line is the 1-based record number in the stream.
	Line int64
}

// ReaderConfig tunes a Reader.
type ReaderConfig struct {
	Dialect Dialect
	// MaxRecordBytes bounds the bytes held for a single record
	// (including terminators). Exceeding it yields *ErrRecordTooLarge.
	// Must be > 0.
	MaxRecordBytes int
	// Lenient allows skipping malformed records (bad quoting,
	// oversized records) instead of failing the stream.
	Lenient bool
	// OnError, if non-nil, is invoked with each recoverable record
	// error that was skipped in lenient mode.
	OnError func(err error)
}

// Reader parses a delimited stream incrementally. Memory usage is
// bounded by MaxRecordBytes and does not grow with input length.
type Reader struct {
}

// NewReader wraps r. A leading UTF-8 BOM, if present, is stripped and
// never appears in field content. Mixed LF/CRLF/CR line endings within
// one stream are accepted and never leak into fields.
func NewReader(r io.Reader, cfg ReaderConfig) (*Reader, error) {
	return &Reader{}, nil
}

// Read returns the next record. It returns io.EOF when the stream is
// exhausted. In strict mode malformed records return a typed error;
// in lenient mode they are reported via OnError and skipped.
func (r *Reader) Read() (Record, error) {
	return Record{}, io.EOF
}

// Position reports the offset of the next unread byte, suitable for
// checkpointing.
func (r *Reader) Position() int64 { return 0 }
