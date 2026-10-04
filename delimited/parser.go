package delimited

import (
	"io"
)

// parseState is the state of the record state machine.
type parseState int

const (
	stRecordStart parseState = iota
	stFieldStart
	stUnquoted
	stQuoted
	stQuotedQuote
	stQuotedWS
)

// Parser is a streaming delimited-text parser. It reads from an io.Reader
// and emits one Record at a time. Memory usage is bounded by the configured
// maximum record size plus a small read buffer: it never materializes the
// whole input.
//
// The parser is a byte-oriented state machine with the following states:
//
//	stRecordStart  beginning of a record (blank lines are skipped)
//	stFieldStart   beginning of a field
//	stUnquoted     reading an unquoted field
//	stQuoted       inside a quoted field
//	stQuotedQuote  a quote was seen inside a quoted field: it is either an
//	               escaped quote ("") or the closing quote
//	stQuotedWS     whitespace between a closing quote and the delimiter
//
// On a recoverable error (ErrRecordTooLong, ErrMalformedQuote) the parser
// skips the remainder of the offending record and is immediately usable
// again.
type Parser struct {
	r   io.Reader
	f   Format
	max int

	buf        []byte
	p          int // next byte to consume
	n          int // number of valid bytes in buf
	baseOffset int64

	pos Position // position of the next byte to consume (line is 1-based)

	state parseState

	recStart   Position
	fieldStart Position
	fields     []string
	field      []byte
	recBytes   int64
	recIndex   int64

	// skipping is set while discarding the remainder of a record that
	// already produced a recoverable error.
	skipping   bool
	pendingErr error

	blankLines int64
	bomChecked bool
	eof        bool
	done       bool
	fatal      error
}

const parserBufferSize = 64 * 1024

// NewParser returns a Parser reading from r in the given format. maxRecordBytes
// limits the size of a single record; a zero or negative value uses 1 MiB.
func NewParser(r io.Reader, f Format, maxRecordBytes int) *Parser {
	if maxRecordBytes <= 0 {
		maxRecordBytes = 1 << 20
	}
	return &Parser{
		r:   r,
		f:   f,
		max: maxRecordBytes,
		buf: make([]byte, parserBufferSize),
		pos: Position{Line: 1},
	}
}

// SetBaseOffset sets the absolute byte offset that position tracking starts
// from. It is used when parsing a sub-range (shard) of a larger input so that
// reported positions are absolute. It must be called before the first Next.
func (p *Parser) SetBaseOffset(off int64) {
	p.baseOffset = off
	p.pos.Offset = off
}

// BlankLines returns the number of blank lines skipped so far.
func (p *Parser) BlankLines() int64 { return p.blankLines }

// Next returns the next record. It returns io.EOF when the input is
// exhausted. Recoverable errors are returned once; the parser continues with
// the following record.
func (p *Parser) Next() (Record, error) {
	if p.done {
		return Record{}, io.EOF
	}
	for {
		b, ok := p.peek()
		if !ok {
			return p.atEOF()
		}
		switch p.state {
		case stRecordStart:
			if !p.bomChecked {
				p.bomChecked = true
				if p.pos.Offset == 0 {
					if err := p.checkBOM(); err != nil {
						p.fatal = err
						return Record{}, err
					}
					if p.fatal != nil {
						return Record{}, p.fatal
					}
					continue
				}
			}
			if isTerm(b) {
				p.consumeTerm(b)
				p.blankLines++
				p.resetRecord()
				continue
			}
			p.recStart = p.pos
			p.recBytes = 0
			p.state = stFieldStart
			// reprocess the byte in stFieldStart
			continue

		case stFieldStart:
			if p.isQuote(b) {
				p.fieldStart = p.pos
				p.consumeByte()
				p.field = p.field[:0]
				p.state = stQuoted
				continue
			}
			if p.isDelim(b) {
				p.consumeByte()
				p.endField()
				continue
			}
			if isTerm(b) {
				p.consumeTerm(b)
				return p.finishRecord()
			}
			if b == ' ' || b == '\t' {
				// skip leading whitespace before a field
				p.consumeByte()
				continue
			}
			b, n := p.consumeByte()
			p.appendByte(b, n)
			p.state = stUnquoted
			continue

		case stUnquoted:
			if p.isDelim(b) {
				p.consumeByte()
				p.endField()
				p.state = stFieldStart
				continue
			}
			if isTerm(b) {
				p.consumeTerm(b)
				return p.finishRecord()
			}
			if p.isQuote(b) {
				p.fail(&ErrMalformedQuote{At: p.pos, Msg: "quote character inside unquoted field"})
				p.consumeByte()
				continue
			}
			if p.f.Escape == EscapeBackslash && b == '\\' {
				p.consumeByte()
				if b2, ok := p.peek(); ok && (p.isQuote(b2) || b2 == '\\') {
					p.consumeByte()
					p.appendByte(b2, 1)
				} else {
					p.appendByte('\\', 1)
				}
				continue
			}
			b, n := p.consumeByte()
			p.appendByte(b, n)
			continue

		case stQuoted:
			if p.isQuote(b) {
				p.consumeByte()
				p.state = stQuotedQuote
				continue
			}
			if p.f.Escape == EscapeBackslash && b == '\\' {
				p.consumeByte()
				if b2, ok := p.peek(); ok && (p.isQuote(b2) || b2 == '\\') {
					p.consumeByte()
					p.appendByte(b2, 1)
				} else {
					p.appendByte('\\', 1)
				}
				continue
			}
			b, n := p.consumeByte()
			p.appendByte(b, n)
			continue

		case stQuotedQuote:
			if p.isQuote(b) {
				// escaped quote ("")
				p.consumeByte()
				p.appendByte(byte(p.f.Quote), 1)
				p.state = stQuoted
				continue
			}
			if p.isDelim(b) {
				p.consumeByte()
				p.endField()
				p.state = stFieldStart
				continue
			}
			if isTerm(b) {
				p.consumeTerm(b)
				return p.finishRecord()
			}
			if b == ' ' || b == '\t' {
				p.consumeByte()
				p.state = stQuotedWS
				continue
			}
			p.fail(&ErrMalformedQuote{At: p.pos, Msg: "unexpected character after closing quote"})
			p.consumeByte()
			continue

		case stQuotedWS:
			if b == ' ' || b == '\t' {
				p.consumeByte()
				continue
			}
			if p.isDelim(b) {
				p.consumeByte()
				p.endField()
				p.state = stFieldStart
				continue
			}
			if isTerm(b) {
				p.consumeTerm(b)
				return p.finishRecord()
			}
			p.fail(&ErrMalformedQuote{At: p.pos, Msg: "unexpected character after closing quote"})
			p.consumeByte()
			continue
		}
	}
}

// fail records a recoverable error and switches to record-skipping mode.
func (p *Parser) fail(err error) {
	if p.pendingErr == nil {
		p.pendingErr = err
	}
	p.skipping = true
}

// finishRecord emits the current record (or returns a pending error) and
// resets the parser for the next record.
func (p *Parser) finishRecord() (Record, error) {
	if p.skipping {
		err := p.pendingErr
		p.resetRecord()
		return Record{}, err
	}
	p.endField()
	rec := p.makeRecord()
	p.recIndex++
	p.resetRecord()
	return rec, nil
}

// makeRecord builds a record with a private copy of the field slice so later
// records cannot alias it.
func (p *Parser) makeRecord() Record {
	return Record{
		Fields: append([]string(nil), p.fields...),
		Pos:    p.recStart,
		EndPos: p.pos,
		Index:  p.recIndex,
	}
}

// atEOF handles end of input.
func (p *Parser) atEOF() (Record, error) {
	if p.fatal != nil {
		p.done = true
		return Record{}, p.fatal
	}
	switch p.state {
	case stRecordStart:
		p.done = true
		return Record{}, io.EOF
	case stFieldStart:
		if p.skipping {
			err := p.pendingErr
			p.resetRecord()
			return Record{}, err
		}
		// trailing empty field after a delimiter
		p.endField()
		rec := p.makeRecord()
		p.recIndex++
		p.resetRecord()
		return rec, nil
	case stUnquoted:
		if p.skipping {
			err := p.pendingErr
			p.resetRecord()
			return Record{}, err
		}
		p.endField()
		rec := p.makeRecord()
		p.recIndex++
		p.resetRecord()
		return rec, nil
	case stQuoted:
		if p.skipping {
			err := p.pendingErr
			p.resetRecord()
			return Record{}, err
		}
		p.done = true
		return Record{}, &ErrUnclosedQuote{At: p.fieldStartPos()}
	case stQuotedQuote, stQuotedWS:
		if p.skipping {
			err := p.pendingErr
			p.resetRecord()
			return Record{}, err
		}
		p.endField()
		rec := p.makeRecord()
		p.recIndex++
		p.resetRecord()
		return rec, nil
	}
	p.done = true
	return Record{}, io.EOF
}

// fieldStartPos returns the position of the opening quote of the current
// field (used for unclosed quote errors).
func (p *Parser) fieldStartPos() Position {
	if p.fieldStart.Offset != 0 || p.fieldStart.Line != 0 {
		return p.fieldStart
	}
	return p.recStart
}

func (p *Parser) endField() {
	p.fields = append(p.fields, string(p.field))
	p.field = p.field[:0]
}

func (p *Parser) resetRecord() {
	p.state = stRecordStart
	p.fields = p.fields[:0]
	p.field = p.field[:0]
	p.recBytes = 0
	p.skipping = false
	p.pendingErr = nil
}

// appendByte appends a consumed byte to the current field and enforces the
// record size limit. b is the first byte returned by consumeByte; n is the
// number of bytes consumed (2 for a CRLF pair inside a quoted field).
func (p *Parser) appendByte(b byte, n int) {
	if p.skipping {
		return
	}
	p.field = append(p.field, b)
	if n == 2 {
		p.field = append(p.field, '\n')
	}
	if p.recBytes > int64(p.max) {
		p.fail(&ErrRecordTooLong{At: p.recStart, Limit: p.max})
	}
}

// consumeByte consumes the next byte, collapsing a CRLF pair into a single
// logical byte for line counting. It returns the first byte and the number of
// bytes consumed (2 for a CRLF pair).
func (p *Parser) consumeByte() (byte, int) {
	b := p.buf[p.p]
	p.p++
	p.pos.Offset++
	n := 1
	if b == '\n' {
		p.pos.Line++
	} else if b == '\r' {
		if p.p >= p.n {
			_ = p.fill()
		}
		if p.p < p.n && p.buf[p.p] == '\n' {
			p.p++
			p.pos.Offset++
			p.pos.Line++
			n = 2
		}
	}
	p.recBytes += int64(n)
	return b, n
}

// consumeTerm consumes a record terminator (LF, CR or CRLF).
func (p *Parser) consumeTerm(b byte) {
	p.consumeByte()
}

// peek returns the next byte without consuming it. It refills the buffer as
// needed. It returns false at EOF or on a fatal read error.
func (p *Parser) peek() (byte, bool) {
	for p.p >= p.n {
		if p.eof {
			return 0, false
		}
		if err := p.fill(); err != nil {
			if err == io.EOF {
				p.eof = true
				return 0, false
			}
			p.fatal = err
			return 0, false
		}
	}
	return p.buf[p.p], true
}

// fill refills the buffer, moving unconsumed bytes to the front.
func (p *Parser) fill() error {
	if p.eof {
		return io.EOF
	}
	if p.p > 0 {
		copy(p.buf, p.buf[p.p:p.n])
		p.n -= p.p
		p.p = 0
	}
	for p.n < len(p.buf) {
		n, err := p.r.Read(p.buf[p.n:])
		p.n += n
		if err != nil {
			if err == io.EOF {
				p.eof = true
				return nil
			}
			return err
		}
	}
	return nil
}

// checkBOM inspects the first bytes of the stream and consumes a UTF-8 BOM.
// It returns ErrUnsupportedEncoding for UTF-16 BOMs.
func (p *Parser) checkBOM() error {
	if p.n < 2 {
		return nil
	}
	if p.buf[0] == 0xFF && p.buf[1] == 0xFE {
		return &ErrUnsupportedEncoding{Encoding: "UTF-16LE"}
	}
	if p.buf[0] == 0xFE && p.buf[1] == 0xFF {
		return &ErrUnsupportedEncoding{Encoding: "UTF-16BE"}
	}
	if p.n >= 3 && p.buf[0] == 0xEF && p.buf[1] == 0xBB && p.buf[2] == 0xBF {
		p.p = 3
		p.pos.Offset = p.baseOffset + 3
	}
	return nil
}

func isTerm(b byte) bool { return b == '\n' || b == '\r' }

func (p *Parser) isDelim(b byte) bool {
	return p.f.Delimiter != 0 && rune(b) == p.f.Delimiter
}

func (p *Parser) isQuote(b byte) bool {
	return p.f.Quote != 0 && rune(b) == p.f.Quote
}
