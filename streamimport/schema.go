package streamimport

import "fmt"

// RaggedPolicy decides how records whose field count differs from the
// expected column count are handled.
type RaggedPolicy int

const (
	// RaggedReject fails (strict) or skips (lenient) the record.
	RaggedReject RaggedPolicy = iota
	// RaggedPad fills missing trailing fields with "". Surplus fields
	// are rejected unless TruncateExtra is set, so data is never
	// silently dropped.
	RaggedPad
	// RaggedDrop discards the record (counted as skipped, reported
	// via OnError) without failing the import.
	RaggedDrop
)

// Schema validates and maps records against a header or fixed width.
type Schema struct {
	// Columns are the expected column names, in order. When empty and
	// the dialect declares a header, the header row defines them.
	Columns []string
	// ExpectedWidth, when > 0, overrides len(Columns) for validation.
	ExpectedWidth int
	// Policy selects ragged-record handling.
	Policy RaggedPolicy
	// TruncateExtra allows RaggedPad to drop surplus trailing fields.
	// It is ignored unless Policy == RaggedPad. Truncation is never
	// silent: callers must report it (the Importer does via OnError).
	TruncateExtra bool

	header []string // effective header, once known
}

// Header returns the effective column names.
func (s *Schema) Header() []string {
	if s.header != nil {
		return s.header
	}
	return s.Columns
}

// Width returns the expected number of fields per record, or 0 when
// no width constraint is known yet.
func (s *Schema) Width() int {
	if s.ExpectedWidth > 0 {
		return s.ExpectedWidth
	}
	if h := s.Header(); len(h) > 0 {
		return len(h)
	}
	return 0
}

// ApplyHeader records a header row parsed from the stream. Explicitly
// configured Columns take precedence; a header row whose width differs
// from them is an error.
func (s *Schema) ApplyHeader(fields []string) error {
	if len(s.Columns) > 0 {
		if len(fields) != len(s.Columns) {
			return fmt.Errorf("streamimport: header has %d columns, expected %d",
				len(fields), len(s.Columns))
		}
		s.header = s.Columns
		return nil
	}
	cp := make([]string, len(fields))
	copy(cp, fields)
	s.header = cp
	return nil
}

// Normalize brings rec to the expected width per Policy, or returns a
// typed error (*ErrColumnCount). It never silently discards data:
// surplus fields are only removed when Policy==RaggedPad and
// TruncateExtra are both set.
func (s *Schema) Normalize(rec Record) (Record, error) {
	w := s.Width()
	if w == 0 || len(rec.Fields) == w {
		return rec, nil
	}
	pos := Position{Offset: rec.Offset, Line: rec.Line}
	switch s.Policy {
	case RaggedPad:
		if len(rec.Fields) < w {
			padded := make([]string, w)
			copy(padded, rec.Fields)
			rec.Fields = padded
			return rec, nil
		}
		if s.TruncateExtra {
			rec.Fields = rec.Fields[:w]
			return rec, nil
		}
		return rec, &ErrColumnCount{Position: pos, Expected: w, Got: len(rec.Fields)}
	case RaggedDrop, RaggedReject:
		return rec, &ErrColumnCount{Position: pos, Expected: w, Got: len(rec.Fields)}
	}
	return rec, &ErrColumnCount{Position: pos, Expected: w, Got: len(rec.Fields)}
}

// Map converts a record into a column->value map using Header().
// Fields beyond the header are ignored only when the schema permits
// truncation; callers should Normalize first.
func (s *Schema) Map(rec Record) map[string]string {
	h := s.Header()
	m := make(map[string]string, len(h))
	for i, name := range h {
		if i < len(rec.Fields) {
			m[name] = rec.Fields[i]
		} else {
			m[name] = ""
		}
	}
	return m
}
