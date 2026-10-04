package delimited

// EscapeStyle describes how quotes are escaped inside quoted fields.
type EscapeStyle int

const (
	// EscapeDoubleQuote escapes a quote by doubling it (""). This is the
	// standard CSV escaping rule and the default.
	EscapeDoubleQuote EscapeStyle = iota
	// EscapeBackslash escapes a quote with a backslash (\").
	EscapeBackslash
	// EscapeNone disables quote escaping.
	EscapeNone
)

// String returns the name of the escape style.
func (e EscapeStyle) String() string {
	switch e {
	case EscapeDoubleQuote:
		return "double-quote"
	case EscapeBackslash:
		return "backslash"
	case EscapeNone:
		return "none"
	default:
		return "unknown"
	}
}

// LineEnding describes the line ending style of an input.
type LineEnding int

const (
	// LineEndingAuto means the parser accepts any mix of LF, CRLF and CR.
	LineEndingAuto LineEnding = iota
	// LineEndingLF means Unix line endings (\n).
	LineEndingLF
	// LineEndingCRLF means Windows line endings (\r\n).
	LineEndingCRLF
	// LineEndingCR means classic Mac line endings (\r).
	LineEndingCR
)

// String returns the name of the line ending style.
func (l LineEnding) String() string {
	switch l {
	case LineEndingLF:
		return "LF"
	case LineEndingCRLF:
		return "CRLF"
	case LineEndingCR:
		return "CR"
	default:
		return "auto"
	}
}

// Format describes the delimited dialect of an input.
type Format struct {
	// Delimiter is the field separator. Common values are ',', '\t', ';',
	// '|' and ':'.
	Delimiter rune
	// Quote is the quoting character. Zero disables quoting.
	Quote rune
	// Escape is the quote escaping style.
	Escape EscapeStyle
	// LineEnding is the dominant line ending style. The parser always accepts
	// mixed line endings regardless of this value; it is recorded for
	// reporting and checkpointing.
	LineEnding LineEnding
	// Confidence is a value in [0,1] indicating how confident detection was.
	// Explicitly supplied formats always have confidence 1.
	Confidence float64
	// Uncertain is true when detection could not determine the format with
	// confidence. Callers must supply an explicit Format override.
	Uncertain bool
	// Reasons explains why detection was uncertain or which signals were used.
	Reasons []string
}

// DefaultFormat returns the standard CSV format: comma delimiter, double
// quote escaping, mixed line endings accepted.
func DefaultFormat() Format {
	return Format{
		Delimiter:  ',',
		Quote:      '"',
		Escape:     EscapeDoubleQuote,
		LineEnding: LineEndingAuto,
		Confidence: 1,
	}
}

// MismatchPolicy controls what happens when a record has a different number
// of fields than the schema.
type MismatchPolicy int

const (
	// MismatchReject rejects mismatching records with ErrColumnMismatch.
	// This is the strict default.
	MismatchReject MismatchPolicy = iota
	// MismatchPad pads short records with empty fields and drops extra
	// fields on long records. All adjustments are counted in Stats.
	MismatchPad
	// MismatchDrop drops records that do not match the schema column count.
	// Dropped records are counted in Stats.
	MismatchDrop
)

// Config controls the behavior of a Reader or Importer.
type Config struct {
	// Format is the explicit dialect. When nil the format is detected
	// automatically from the input sample.
	Format *Format
	// SampleSize is the number of bytes used for automatic format
	// detection. Zero uses a sensible default (64 KiB).
	SampleSize int
	// MaxRecordBytes is the maximum size of a single record in bytes. Zero
	// uses a sensible default (1 MiB). Records exceeding the limit produce a
	// recoverable ErrRecordTooLong.
	MaxRecordBytes int
	// Strict enables strict parsing: malformed quotes and column mismatches
	// abort the stream. When false (lenient) bad records are skipped and
	// counted.
	Strict bool
	// OnMismatch controls column count mismatches when a schema is present.
	// Defaults to MismatchReject.
	OnMismatch MismatchPolicy
	// BatchSize is the number of records handed to the Sink per Write call.
	// Zero uses a sensible default (128).
	BatchSize int
	// ShardCount is the number of concurrent processing shards. Zero uses
	// runtime.GOMAXPROCS. Sharding requires a seekable Source.
	ShardCount int
	// NoHeaderRow disables treating the first record as a header row.
	// When false (default) the first record is consumed as the header used
	// for column mapping. Ignored when Schema is explicitly set.
	NoHeaderRow bool
	// Schema is an explicit column schema. When set, HeaderRow is ignored.
	Schema *Schema
	// OnError is called for every recoverable error in lenient mode
	// (skipped records, column adjustments). It is never called for fatal
	// errors. May be nil.
	OnError func(error)
}

// withDefaults returns a copy of the config with zero values replaced by
// defaults.
func (c Config) withDefaults() Config {
	if c.SampleSize == 0 {
		c.SampleSize = 64 * 1024
	}
	if c.MaxRecordBytes == 0 {
		c.MaxRecordBytes = 1 << 20
	}
	if c.BatchSize == 0 {
		c.BatchSize = 128
	}
	return c
}
