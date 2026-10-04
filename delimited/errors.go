package delimited

import (
	"errors"
	"fmt"
)

// Position identifies a byte location in the input stream.
type Position struct {
	// Offset is the zero-based byte offset from the start of the input.
	Offset int64
	// Line is the one-based line number. A line is counted after every
	// consumed line terminator (LF, CR or CRLF).
	Line int64
}

// String returns a human readable representation of the position.
func (p Position) String() string {
	return fmt.Sprintf("offset %d, line %d", p.Offset, p.Line)
}

// ParseError is implemented by all errors that carry a stream position.
type ParseError interface {
	error
	Pos() Position
}

// ErrRecordTooLong is returned when a record exceeds the configured maximum
// size. The parser skips the remainder of the record and is immediately
// usable again: the error is recoverable.
type ErrRecordTooLong struct {
	At    Position
	Limit int
}

func (e *ErrRecordTooLong) Error() string {
	return fmt.Sprintf("delimited: record starting at %s exceeds maximum size of %d bytes", e.At, e.Limit)
}

func (e *ErrRecordTooLong) Pos() Position { return e.At }

// ErrMalformedQuote is returned in strict mode when a quote character appears
// in a position that cannot be a valid field delimiter (e.g. inside an
// unquoted field). In lenient mode the character is treated as literal data.
type ErrMalformedQuote struct {
	At  Position
	Msg string
}

func (e *ErrMalformedQuote) Error() string {
	if e.Msg != "" {
		return fmt.Sprintf("delimited: malformed quote at %s: %s", e.At, e.Msg)
	}
	return fmt.Sprintf("delimited: malformed quote at %s", e.At)
}

func (e *ErrMalformedQuote) Pos() Position { return e.At }

// ErrUnclosedQuote is returned when the input ends while a quoted field is
// still open. It is a terminal error for the current stream.
type ErrUnclosedQuote struct {
	At Position
}

func (e *ErrUnclosedQuote) Error() string {
	return fmt.Sprintf("delimited: unclosed quoted field starting at %s at end of input", e.At)
}

func (e *ErrUnclosedQuote) Pos() Position { return e.At }

// ErrColumnMismatch is returned when a record's field count does not match
// the schema's column count under the strict (reject) policy.
type ErrColumnMismatch struct {
	At       Position
	Expected int
	Actual   int
}

func (e *ErrColumnMismatch) Error() string {
	return fmt.Sprintf("delimited: column count mismatch at %s: expected %d columns, got %d", e.At, e.Expected, e.Actual)
}

func (e *ErrColumnMismatch) Pos() Position { return e.At }

// ErrDuplicateHeader is returned when a header name appears more than once.
type ErrDuplicateHeader struct {
	Name string
}

func (e *ErrDuplicateHeader) Error() string {
	return fmt.Sprintf("delimited: duplicate header %q", e.Name)
}

// ErrUncertainFormat is returned when automatic format detection cannot
// determine the delimiter, quote or escape style with confidence. The caller
// must supply an explicit Format override.
type ErrUncertainFormat struct {
	Reasons []string
}

func (e *ErrUncertainFormat) Error() string {
	return fmt.Sprintf("delimited: uncertain input format: %v (provide an explicit Format to override detection)", e.Reasons)
}

// ErrUnsupportedEncoding is returned when the input carries a UTF-16 BOM.
// Only UTF-8 (with optional BOM) is supported.
type ErrUnsupportedEncoding struct {
	Encoding string
}

func (e *ErrUnsupportedEncoding) Error() string {
	return fmt.Sprintf("delimited: unsupported input encoding %s (only UTF-8 is supported)", e.Encoding)
}

// ErrSourceChanged is returned when resuming from a checkpoint whose source
// identity (size) does not match the current input.
type ErrSourceChanged struct {
	CheckpointSize int64
	CurrentSize    int64
}

func (e *ErrSourceChanged) Error() string {
	return fmt.Sprintf("delimited: source changed since checkpoint (checkpoint size %d, current size %d)", e.CheckpointSize, e.CurrentSize)
}

// ErrCheckpointCorrupt is returned when a checkpoint file cannot be decoded.
type ErrCheckpointCorrupt struct {
	Path string
	Err  error
}

func (e *ErrCheckpointCorrupt) Error() string {
	return fmt.Sprintf("delimited: corrupt checkpoint %q: %v", e.Path, e.Err)
}

func (e *ErrCheckpointCorrupt) Unwrap() error { return e.Err }

// Sentinel errors for control flow.
var (
	// ErrAborted is returned when an import is stopped because a shard failed
	// in strict mode.
	ErrAborted = errors.New("delimited: import aborted")
)
