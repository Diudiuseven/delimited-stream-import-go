package delimited

import (
	"bytes"
	"io"
)

// Reader reads delimited records from a stream. It combines the streaming
// Parser with optional schema mapping and a configurable error policy.
//
// In strict mode any parse error or column mismatch aborts the stream. In
// lenient mode bad records are skipped and counted in Stats.
type Reader struct {
	p          *Parser
	cfg        Config
	format     Format
	schema     *Schema
	stats      *Stats
	header     bool
	started    bool
	baseOffset int64
	lastBlank  int64
	lastBytes  int64
}

// NewReader returns a Reader over r. When cfg.Format is nil the dialect is
// detected from the first SampleSize bytes; r need not be seekable. When
// cfg.Schema is nil and NoHeaderRow is false (the default) the first record
// is consumed as the header row.
func NewReader(r io.Reader, cfg Config, stats *Stats) (*Reader, error) {
	cfg = cfg.withDefaults()
	if stats == nil {
		stats = &Stats{}
	}
	rd := &Reader{
		cfg:    cfg,
		stats:  stats,
		header: true,
	}

	if cfg.Format != nil {
		rd.format = *cfg.Format
	} else {
		sample := make([]byte, cfg.SampleSize)
		n, err := io.ReadFull(r, sample)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return nil, err
		}
		sample = sample[:n]
		detected, err := DetectFormatBytes(sample)
		if err != nil {
			return nil, err
		}
		if detected.Uncertain {
			return nil, &ErrUncertainFormat{Reasons: detected.Reasons}
		}
		rd.format = *detected
		r = io.MultiReader(bytes.NewReader(sample), r)
	}

	rd.p = NewParser(r, rd.format, cfg.MaxRecordBytes)

	if cfg.Schema != nil {
		rd.schema = cfg.Schema
		rd.header = false
	} else if cfg.NoHeaderRow {
		rd.header = false
	} else {
		// Consume the header row.
		rec, err := rd.p.Next()
		if err == io.EOF {
			return rd, nil
		}
		if err != nil {
			return nil, err
		}
		rd.schema, err = NewSchema(rec.Fields)
		if err != nil {
			return nil, err
		}
	}
	return rd, nil
}

// SetBaseOffset sets the absolute byte offset that position tracking starts
// from. It is used when reading a sub-range (shard) of a larger input. It
// must be called before the first Read.
func (rd *Reader) SetBaseOffset(off int64) {
	rd.baseOffset = off
	rd.lastBytes = off
	if rd.p != nil {
		rd.p.SetBaseOffset(off)
	}
}

// Format returns the dialect used by the reader.
func (rd *Reader) Format() Format { return rd.format }

// Schema returns the schema used for column mapping, or nil when no schema
// is present.
func (rd *Reader) Schema() *Schema { return rd.schema }

// Stats returns the reader's stats.
func (rd *Reader) Stats() *Stats { return rd.stats }

// BytesRead returns the number of input bytes consumed so far.
func (rd *Reader) BytesRead() int64 { return rd.p.pos.Offset }

// Read returns the next data record. It returns io.EOF at end of input.
// Recoverable errors in lenient mode are skipped automatically; in strict
// mode they are returned.
func (rd *Reader) Read() (Record, error) {
	for {
		rec, err := rd.p.Next()
		if bl := rd.p.BlankLines(); bl != rd.lastBlank {
			rd.stats.addBlankLines(bl - rd.lastBlank)
			rd.lastBlank = bl
		}
		if br := rd.p.pos.Offset; br != rd.lastBytes {
			rd.stats.addBytes(br - rd.lastBytes)
			rd.lastBytes = br
		}
		if err == io.EOF {
			return rec, err
		}
		if err != nil {
			if rd.cfg.Strict {
				return rec, err
			}
			rd.stats.addError()
			rd.stats.addSkipped()
			if rd.cfg.OnError != nil {
				rd.cfg.OnError(err)
			}
			continue
		}
		rd.stats.addRead()
		if rd.schema != nil {
			fields, padded, dropped, err := rd.schema.Map(rec, rd.cfg.OnMismatch)
			if err != nil {
				if rd.cfg.Strict {
					return rec, err
				}
				if rd.cfg.OnMismatch == MismatchDrop {
					rd.stats.addError()
					rd.stats.addSkipped()
					if rd.cfg.OnError != nil {
						rd.cfg.OnError(err)
					}
					continue
				}
				return rec, err
			}
			if padded > 0 {
				rd.stats.addPadded(int64(padded))
			}
			if dropped > 0 {
				rd.stats.addDropped(int64(dropped))
			}
			rec.Fields = fields
		}
		return rec, nil
	}
}

// ReadAll reads all remaining records. It stops at the first error in
// strict mode; in lenient mode it skips bad records and continues.
func (rd *Reader) ReadAll() ([]Record, error) {
	var recs []Record
	for {
		rec, err := rd.Read()
		if err == io.EOF {
			return recs, nil
		}
		if err != nil {
			return recs, err
		}
		recs = append(recs, rec)
	}
}
