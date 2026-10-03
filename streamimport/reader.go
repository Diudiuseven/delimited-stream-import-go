package streamimport

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// Record is one parsed row.
type Record struct {
	// Fields holds the decoded field values.
	Fields []string
	// Offset is the byte offset where this record starts, relative to
	// the first byte after any BOM.
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

	// noBOM disables BOM stripping. Set internally when the reader
	// starts at a mid-stream offset (resume / shard boundary), where
	// leading bytes must never be mistaken for a BOM.
	noBOM bool
}

const readerBufSize = 64 * 1024

var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

type parserState int

const (
	stateFieldStart parserState = iota
	stateUnquoted
	stateQuoted
	stateAfterQuote
)

// Reader parses a delimited stream incrementally. Memory usage is
// bounded by MaxRecordBytes plus a fixed 64 KiB read buffer and does
// not grow with input length.
type Reader struct {
	br   *bufio.Reader
	cfg  ReaderConfig
	d    Dialect
	off  int64 // bytes consumed since the first byte after any BOM
	line int64

	field  []byte   // reusable current-field buffer
	fields []string // reusable fields slice
}

// NewReader wraps r. A leading UTF-8 BOM, if present, is stripped and
// never appears in field content. Mixed LF/CRLF/CR line endings within
// one stream are accepted and never leak into fields. Blank lines
// (lines containing only a terminator) are skipped.
func NewReader(r io.Reader, cfg ReaderConfig) (*Reader, error) {
	if cfg.MaxRecordBytes <= 0 {
		return nil, fmt.Errorf("streamimport: MaxRecordBytes must be > 0, got %d", cfg.MaxRecordBytes)
	}
	if cfg.Dialect.Delimiter == 0 {
		return nil, fmt.Errorf("streamimport: dialect delimiter is not set")
	}
	br := bufio.NewReaderSize(r, readerBufSize)
	rd := &Reader{br: br, cfg: cfg, d: cfg.Dialect}
	if !cfg.noBOM {
		if err := rd.stripBOM(); err != nil {
			return nil, err
		}
	}
	return rd, nil
}

func (r *Reader) stripBOM() error {
	b, err := r.br.Peek(3)
	if err != nil && err != io.EOF && err != bufio.ErrBufferFull {
		return err
	}
	if len(b) == 3 && b[0] == utf8BOM[0] && b[1] == utf8BOM[1] && b[2] == utf8BOM[2] {
		if _, err := r.br.Discard(3); err != nil {
			return err
		}
	}
	return nil
}

// Position reports the offset of the next unread byte, suitable for
// checkpointing. It equals the EndOffset of the last record returned.
func (r *Reader) Position() int64 { return r.off }

// Read returns the next record. It returns io.EOF when the stream is
// exhausted. In strict mode malformed records return a typed error;
// in lenient mode they are reported via OnError and skipped.
func (r *Reader) Read() (Record, error) {
	for {
		rec, err := r.readRecord()
		if err != nil {
			var unclosed *ErrUnclosedQuote
			if r.cfg.Lenient && IsRecoverable(err) {
				if r.cfg.OnError != nil {
					r.cfg.OnError(err)
				}
				if errors.As(err, &unclosed) {
					// Stream ended inside a quoted field: nothing
					// further can be recovered.
					return Record{}, io.EOF
				}
				continue
			}
			return Record{}, err
		}
		return rec, nil
	}
}

// readRecord parses exactly one record (skipping blank lines).
func (r *Reader) readRecord() (Record, error) {
	for {
		rec, blank, err := r.scanRecord()
		if err != nil {
			return Record{}, err
		}
		if blank {
			continue
		}
		return rec, nil
	}
}

// scanRecord runs the state machine until one record terminator.
// blank is true when the "record" was an empty line to be skipped.
func (r *Reader) scanRecord() (rec Record, blank bool, err error) {
	start := r.off
	r.field = r.field[:0]
	r.fields = r.fields[:0]
	state := stateFieldStart
	recBytes := 0
	sawAny := false
	// pendingErr, when non-nil, puts the scanner in discard mode:
	// bytes are no longer stored, but the state machine keeps
	// running so the record boundary is located correctly. At the
	// terminator the pending error is returned instead of a record.
	// Only used in lenient mode.
	var pendingErr error

	endField := func() {
		r.fields = append(r.fields, string(r.field))
		r.field = r.field[:0]
	}
	emit := func() (Record, bool, error) {
		fields := make([]string, len(r.fields))
		copy(fields, r.fields)
		r.line++
		return Record{
			Fields:    fields,
			Offset:    start,
			EndOffset: r.off,
			Line:      r.line,
		}, false, nil
	}

	for {
		c, rerr := r.br.ReadByte()
		if rerr == io.EOF {
			switch state {
			case stateFieldStart:
				if !sawAny && len(r.fields) == 0 {
					return Record{}, false, io.EOF
				}
				// Input ended right after a delimiter: trailing
				// empty field.
				endField()
				return emit()
			case stateUnquoted, stateAfterQuote:
				endField()
				return emit()
			case stateQuoted:
				return Record{}, false, &ErrUnclosedQuote{
					Position: Position{Offset: start, Line: r.line + 1},
				}
			}
		}
		if rerr != nil {
			return Record{}, false, rerr
		}
		r.off++
		sawAny = true
		recBytes++
		if recBytes > r.cfg.MaxRecordBytes && pendingErr == nil {
			if !r.cfg.Lenient {
				return Record{}, false, &ErrRecordTooLarge{
					Position: Position{Offset: start, Line: r.line + 1},
					Limit:    r.cfg.MaxRecordBytes,
				}
			}
			pendingErr = r.tooLargeErr(start)
		}

		if pendingErr != nil {
			// Boundary tracking only; mirror state transitions
			// without storing.
			switch state {
			case stateFieldStart:
				if r.d.Quote != 0 && c == r.d.Quote {
					state = stateQuoted
				} else if c == r.d.Delimiter {
					// stay
				} else if isNewline(c) {
					if c == '\r' {
						r.consumeLF()
					}
					return Record{}, false, pendingErr
				} else {
					state = stateUnquoted
				}
			case stateUnquoted:
				if c == r.d.Delimiter {
					state = stateFieldStart
				} else if isNewline(c) {
					if c == '\r' {
						r.consumeLF()
					}
					return Record{}, false, pendingErr
				}
			case stateQuoted:
				if r.d.Escape != 0 && c == r.d.Escape {
					if _, err := r.br.ReadByte(); err == nil {
						r.off++
					}
				} else if c == r.d.Quote {
					state = stateAfterQuote
				}
			case stateAfterQuote:
				if c == r.d.Delimiter {
					state = stateFieldStart
				} else if isNewline(c) {
					if c == '\r' {
						r.consumeLF()
					}
					return Record{}, false, pendingErr
				} else if r.d.Quote != 0 && c == r.d.Quote {
					state = stateQuoted
				}
			}
			continue
		}

		switch state {
		case stateFieldStart:
			switch {
			case r.d.Quote != 0 && c == r.d.Quote:
				state = stateQuoted
			case c == r.d.Delimiter:
				endField()
			case isNewline(c):
				if c == '\r' {
					r.consumeLF()
				}
				if len(r.fields) == 0 && len(r.field) == 0 {
					return Record{}, true, nil // blank line
				}
				endField()
				return emit()
			default:
				r.field = append(r.field, c)
				state = stateUnquoted
			}
		case stateUnquoted:
			switch {
			case r.d.Escape != 0 && c == r.d.Escape:
				nc, err := r.readEscaped()
				if err != nil {
					return Record{}, false, err
				}
				r.field = append(r.field, nc)
			case c == r.d.Delimiter:
				endField()
				state = stateFieldStart
			case isNewline(c):
				if c == '\r' {
					r.consumeLF()
				}
				endField()
				return emit()
			case r.d.Quote != 0 && c == r.d.Quote:
				bq := &ErrBareQuote{
					Position: Position{Offset: r.off - 1, Line: r.line + 1},
					Detail:   "quote character inside unquoted field",
				}
				if !r.cfg.Lenient {
					return Record{}, false, bq
				}
				pendingErr = bq
			default:
				r.field = append(r.field, c)
			}
		case stateQuoted:
			switch {
			case r.d.Escape != 0 && c == r.d.Escape:
				nc, err := r.readEscaped()
				if err != nil {
					return Record{}, false, err
				}
				r.field = append(r.field, nc)
			case c == r.d.Quote:
				if r.d.Escape == 0 {
					// Doubled-quote escaping: peek for a second quote.
					if nb, err := r.br.Peek(1); err == nil && nb[0] == r.d.Quote {
						_, _ = r.br.ReadByte()
						r.off++
						recBytes++
						r.field = append(r.field, c)
						continue
					}
				}
				state = stateAfterQuote
			default:
				r.field = append(r.field, c)
			}
		case stateAfterQuote:
			switch {
			case c == r.d.Delimiter:
				endField()
				state = stateFieldStart
			case isNewline(c):
				if c == '\r' {
					r.consumeLF()
				}
				endField()
				return emit()
			default:
				bq := &ErrBareQuote{
					Position: Position{Offset: r.off - 1, Line: r.line + 1},
					Detail:   "unexpected character after closing quote",
				}
				if !r.cfg.Lenient {
					return Record{}, false, bq
				}
				pendingErr = bq
			}
		}
	}
}

func (r *Reader) tooLargeErr(start int64) error {
	return &ErrRecordTooLarge{
		Position: Position{Offset: start, Line: r.line + 1},
		Limit:    r.cfg.MaxRecordBytes,
	}
}

// readEscaped reads the byte following an escape character.
func (r *Reader) readEscaped() (byte, error) {
	c, err := r.br.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, &ErrUnclosedQuote{
				Position: Position{Offset: r.off, Line: r.line + 1},
			}
		}
		return 0, err
	}
	r.off++
	return c, nil
}

// consumeLF swallows a '\n' immediately after a consumed '\r'.
func (r *Reader) consumeLF() {
	if nb, err := r.br.Peek(1); err == nil && nb[0] == '\n' {
		_, _ = r.br.ReadByte()
		r.off++
	}
}

func isNewline(c byte) bool { return c == '\n' || c == '\r' }
