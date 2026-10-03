package streamimport

// RaggedPolicy decides how records whose field count differs from the
// expected column count are handled.
type RaggedPolicy int

const (
	// RaggedReject fails (strict) or skips (lenient) the record.
	RaggedReject RaggedPolicy = iota
	// RaggedPad fills missing trailing fields with "" and truncates
	// extras only when TruncateExtra is set; otherwise extras are
	// still rejected so data is never silently dropped.
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
	// It is ignored unless Policy == RaggedPad, and never applies
	// silently: truncation is reported via OnError.
	TruncateExtra bool
}

// Header returns the effective column names.
func (s *Schema) Header() []string { return nil }

// Width returns the expected number of fields per record.
func (s *Schema) Width() int { return 0 }

// ApplyHeader records a header row parsed from the stream.
func (s *Schema) ApplyHeader(fields []string) error { return nil }

// Normalize brings rec to the expected width per Policy, or returns a
// typed error (*ErrColumnCount). It never silently discards data.
func (s *Schema) Normalize(rec Record) (Record, error) { return rec, nil }

// Map converts a record into a column->value map using Header().
func (s *Schema) Map(rec Record) map[string]string { return nil }
