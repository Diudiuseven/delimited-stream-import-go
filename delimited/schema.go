package delimited

import "fmt"

// Schema describes the column layout of a delimited input.
type Schema struct {
	headers []string
	index   map[string]int
}

// NewSchema builds a schema from header names. Duplicate header names are an
// error.
func NewSchema(headers []string) (*Schema, error) {
	s := &Schema{
		headers: append([]string(nil), headers...),
		index:   make(map[string]int, len(headers)),
	}
	for i, h := range headers {
		if _, dup := s.index[h]; dup {
			return nil, &ErrDuplicateHeader{Name: h}
		}
		s.index[h] = i
	}
	return s, nil
}

// Headers returns a copy of the header names in order.
func (s *Schema) Headers() []string {
	return append([]string(nil), s.headers...)
}

// Len returns the number of columns.
func (s *Schema) Len() int { return len(s.headers) }

// Index returns the column index of a header name.
func (s *Schema) Index(name string) (int, bool) {
	i, ok := s.index[name]
	return i, ok
}

// Map aligns a record's fields to the schema, applying the mismatch policy.
// It returns the normalized field slice (len == schema.Len()) and an error
// under MismatchReject. Under MismatchPad short records are padded with empty
// fields and long records are truncated; under MismatchDrop mismatching
// records are rejected with ErrColumnMismatch so the caller can drop them.
// The returned counters report how many fields were padded or dropped.
func (s *Schema) Map(rec Record, policy MismatchPolicy) (fields []string, padded, dropped int, err error) {
	n := len(s.headers)
	got := len(rec.Fields)
	switch {
	case got == n:
		return append([]string(nil), rec.Fields...), 0, 0, nil
	case got < n:
		if policy == MismatchReject {
			return nil, 0, 0, &ErrColumnMismatch{At: rec.Pos, Expected: n, Actual: got}
		}
		if policy == MismatchDrop {
			return nil, 0, 0, &ErrColumnMismatch{At: rec.Pos, Expected: n, Actual: got}
		}
		// MismatchPad: pad with empty fields.
		out := make([]string, n)
		copy(out, rec.Fields)
		return out, n - got, 0, nil
	default: // got > n
		if policy == MismatchReject {
			return nil, 0, 0, &ErrColumnMismatch{At: rec.Pos, Expected: n, Actual: got}
		}
		if policy == MismatchDrop {
			return nil, 0, 0, &ErrColumnMismatch{At: rec.Pos, Expected: n, Actual: got}
		}
		// MismatchPad: truncate extra fields.
		return append([]string(nil), rec.Fields[:n]...), 0, got - n, nil
	}
}

// String returns a debug representation of the schema.
func (s *Schema) String() string {
	return fmt.Sprintf("Schema(%v)", s.headers)
}
