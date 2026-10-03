package streamimport

// LineEnding describes the line terminator style of an input.
type LineEnding int

const (
	LineEndingUnknown LineEnding = iota
	LineEndingLF
	LineEndingCRLF
	LineEndingCR
	LineEndingMixed
)

func (l LineEnding) String() string { return "" }

// Dialect describes how a delimited text stream is tokenized.
// A zero Quote disables quote handling. A zero Escape means
// "doubled quote" escaping when Quote is set.
type Dialect struct {
	Delimiter  byte
	Quote      byte
	Escape     byte
	LineEnding LineEnding
	HasHeader  bool
}

// DialectOverride carries explicitly supplied dialect fields.
// Any non-zero field wins over detection; detection fills the rest.
type DialectOverride struct {
	Delimiter  *byte
	Quote      *byte
	Escape     *byte
	LineEnding *LineEnding
	HasHeader  *bool
}

// ErrAmbiguousDialect is returned when detection cannot decide.
type ErrAmbiguousDialect struct {
	Reason string
}

func (e *ErrAmbiguousDialect) Error() string { return "" }

// DetectDialect infers the dialect from a sample. It is deterministic:
// the same sample always yields the same result. It returns
// *ErrAmbiguousDialect when the evidence is insufficient instead of
// silently guessing.
func DetectDialect(sample []byte) (Dialect, error) {
	return Dialect{}, nil
}

// Apply merges explicit overrides on top of a detected dialect.
func (d Dialect) Apply(o DialectOverride) Dialect { return d }
