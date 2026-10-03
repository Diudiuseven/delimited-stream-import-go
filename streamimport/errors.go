package streamimport

import (
	"errors"
	"fmt"
)

// Position identifies a location in the input stream.
type Position struct {
	// Offset is the absolute byte offset in the source (after any BOM).
	Offset int64
	// Line is the 1-based record line number.
	Line int64
}

func (p Position) String() string {
	return fmt.Sprintf("line %d, offset %d", p.Line, p.Offset)
}

// ErrRecordTooLarge is returned when a single record exceeds the
// configured MaxRecordBytes. It is recoverable: in lenient mode the
// offending record is skipped and reading continues.
type ErrRecordTooLarge struct {
	Position Position
	Limit    int
}

func (e *ErrRecordTooLarge) Error() string {
	return fmt.Sprintf("streamimport: record at %s exceeds max record size %d bytes",
		e.Position, e.Limit)
}

// ErrBareQuote is returned for malformed quoting, e.g. a quote character
// appearing in the middle of an unquoted field, or characters following
// a closing quote other than delimiter/newline.
type ErrBareQuote struct {
	Position Position
	Detail   string
}

func (e *ErrBareQuote) Error() string {
	return fmt.Sprintf("streamimport: malformed quote at %s: %s", e.Position, e.Detail)
}

// ErrUnclosedQuote is returned when the input ends inside a quoted field.
type ErrUnclosedQuote struct {
	Position Position // where the quoted field started
}

func (e *ErrUnclosedQuote) Error() string {
	return fmt.Sprintf("streamimport: unclosed quoted field starting at %s", e.Position)
}

// ErrColumnCount is returned when a record's field count differs from
// the expected column count and the ragged policy rejects it.
type ErrColumnCount struct {
	Position Position
	Expected int
	Got      int
}

func (e *ErrColumnCount) Error() string {
	return fmt.Sprintf("streamimport: record at %s has %d fields, expected %d",
		e.Position, e.Got, e.Expected)
}

// IsRecoverable reports whether err is a per-record error that lenient
// mode can skip past (as opposed to a fatal I/O or usage error).
func IsRecoverable(err error) bool {
	var tl *ErrRecordTooLarge
	var bq *ErrBareQuote
	var cc *ErrColumnCount
	return errors.As(err, &tl) || errors.As(err, &bq) || errors.As(err, &cc)
}
