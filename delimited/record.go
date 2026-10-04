package delimited

// Record is a single parsed record.
type Record struct {
	// Fields are the raw field values, in input order.
	Fields []string
	// Pos is the position of the first byte of the record.
	Pos Position
	// EndPos is the position immediately after the record terminator.
	EndPos Position
	// Index is the zero-based record index in the stream, counting only
	// records that were emitted (skipped records are not counted).
	Index int64
}

// Field returns the field at index i, or "" if i is out of range.
func (r Record) Field(i int) string {
	if i < 0 || i >= len(r.Fields) {
		return ""
	}
	return r.Fields[i]
}

// Get returns the value of the named column using the schema, and whether
// the column exists. The record must have been mapped against the schema.
func (r Record) Get(s *Schema, name string) (string, bool) {
	if s == nil {
		return "", false
	}
	idx, ok := s.Index(name)
	if !ok {
		return "", false
	}
	if idx >= len(r.Fields) {
		return "", false
	}
	return r.Fields[idx], true
}
