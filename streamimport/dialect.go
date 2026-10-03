package streamimport

import (
	"bytes"
	"fmt"
	"sort"
)

// LineEnding describes the line terminator style of an input.
type LineEnding int

const (
	LineEndingUnknown LineEnding = iota
	LineEndingLF
	LineEndingCRLF
	LineEndingCR
	LineEndingMixed
)

func (l LineEnding) String() string {
	switch l {
	case LineEndingLF:
		return "LF"
	case LineEndingCRLF:
		return "CRLF"
	case LineEndingCR:
		return "CR"
	case LineEndingMixed:
		return "Mixed"
	default:
		return "Unknown"
	}
}

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
// Any non-nil field wins over detection; detection fills the rest.
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

func (e *ErrAmbiguousDialect) Error() string {
	return "streamimport: ambiguous dialect: " + e.Reason
}

// Apply merges explicit overrides on top of a detected dialect.
func (d Dialect) Apply(o DialectOverride) Dialect {
	if o.Delimiter != nil {
		d.Delimiter = *o.Delimiter
	}
	if o.Quote != nil {
		d.Quote = *o.Quote
	}
	if o.Escape != nil {
		d.Escape = *o.Escape
	}
	if o.LineEnding != nil {
		d.LineEnding = *o.LineEnding
	}
	if o.HasHeader != nil {
		d.HasHeader = *o.HasHeader
	}
	return d
}

// candidate delimiters in fixed priority order for tie-breaking, so
// results are deterministic.
var delimiterCandidates = []byte{',', '\t', ';', '|', ':'}

// quoteCandidates in fixed priority order.
var quoteCandidates = []byte{'"', '\''}

// DetectDialect infers the dialect from a sample. It is deterministic:
// the same sample always yields the same result. It returns
// *ErrAmbiguousDialect when the evidence is insufficient instead of
// silently guessing; callers may then supply a DialectOverride.
func DetectDialect(sample []byte) (Dialect, error) {
	// Strip a UTF-8 BOM before analysis so it cannot skew counts.
	sample = bytes.TrimPrefix(sample, []byte{0xEF, 0xBB, 0xBF})
	if len(bytes.TrimSpace(sample)) == 0 {
		return Dialect{}, &ErrAmbiguousDialect{Reason: "sample is empty"}
	}

	d := Dialect{LineEnding: detectLineEnding(sample)}

	lines := splitSampleLines(sample)
	if len(lines) == 0 {
		return Dialect{}, &ErrAmbiguousDialect{Reason: "sample contains no complete line"}
	}

	delim, err := detectDelimiter(lines)
	if err != nil {
		return Dialect{}, err
	}
	d.Delimiter = delim

	d.Quote = detectQuote(sample, delim)
	d.HasHeader = detectHeader(lines, delim)
	return d, nil
}

// detectLineEnding counts terminator styles outside of quoted regions
// is unnecessary here: a terminator byte is a terminator regardless;
// quoted newlines are rare in samples and both CR and LF are counted
// per-occurrence, so embedded newlines only add noise, not bias.
func detectLineEnding(sample []byte) LineEnding {
	var crlf, lfOnly, crOnly int
	for i := 0; i < len(sample); i++ {
		switch sample[i] {
		case '\r':
			if i+1 < len(sample) && sample[i+1] == '\n' {
				crlf++
				i++
			} else {
				crOnly++
			}
		case '\n':
			lfOnly++
		}
	}
	total := crlf + lfOnly + crOnly
	if total == 0 {
		return LineEndingUnknown
	}
	// A style dominates if it accounts for >= 90% of terminators.
	dominant := func(n int) bool { return n*10 >= total*9 }
	switch {
	case dominant(crlf):
		return LineEndingCRLF
	case dominant(lfOnly):
		return LineEndingLF
	case dominant(crOnly):
		return LineEndingCR
	default:
		return LineEndingMixed
	}
}

// splitSampleLines splits on any terminator style and drops the final
// partial line (which may be truncated mid-record).
func splitSampleLines(sample []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i := 0; i < len(sample); i++ {
		if sample[i] == '\n' || sample[i] == '\r' {
			lines = append(lines, sample[start:i])
			if sample[i] == '\r' && i+1 < len(sample) && sample[i+1] == '\n' {
				i++
			}
			start = i + 1
		}
	}
	// Ignore the trailing partial line; it may be cut mid-record.
	return lines
}

// countOutsideQuotes counts occurrences of c on a line, ignoring those
// inside either quote character (doubled quotes count as escaped).
func countOutsideQuotes(line []byte, c byte) int {
	var in byte
	n := 0
	for i := 0; i < len(line); i++ {
		ch := line[i]
		if in != 0 {
			if ch == in {
				if i+1 < len(line) && line[i+1] == in {
					i++ // doubled quote: escaped
					continue
				}
				in = 0
			}
			continue
		}
		if ch == '"' || ch == '\'' {
			in = ch
			continue
		}
		if ch == c {
			n++
		}
	}
	return n
}

// detectDelimiter scores each candidate by consistency of per-line
// field counts: a real delimiter produces the same positive count on
// (nearly) every line.
func detectDelimiter(lines [][]byte) (byte, error) {
	type score struct {
		delim byte
		// consistent is the number of lines sharing the modal count.
		consistent int
		count      int // modal per-line count
	}
	scores := make([]score, 0, len(delimiterCandidates))
	for _, c := range delimiterCandidates {
		hist := map[int]int{}
		for _, ln := range lines {
			if len(bytes.TrimSpace(ln)) == 0 {
				continue
			}
			hist[countOutsideQuotes(ln, c)]++
		}
		best := score{delim: c}
		for cnt, freq := range hist {
			if cnt > 0 && (freq > best.consistent || (freq == best.consistent && cnt > best.count)) {
				best.consistent = freq
				best.count = cnt
			}
		}
		scores = append(scores, best)
	}
	// Sort: most consistent lines first, then higher field count, then
	// candidate priority order (stable, deterministic).
	sort.SliceStable(scores, func(i, j int) bool {
		if scores[i].consistent != scores[j].consistent {
			return scores[i].consistent > scores[j].consistent
		}
		return scores[i].count > scores[j].count
	})
	top := scores[0]
	if top.consistent == 0 {
		return 0, &ErrAmbiguousDialect{Reason: "no delimiter candidate produces a consistent column count"}
	}
	// Require the winner to be strictly better than the runner-up, or
	// the runner-up to be clearly inconsistent (zero).
	if len(scores) > 1 && scores[1].consistent == top.consistent && scores[1].count == top.count {
		return 0, &ErrAmbiguousDialect{
			Reason: fmt.Sprintf("delimiters %q and %q score equally", top.delim, scores[1].delim),
		}
	}
	return top.delim, nil
}

// detectQuote picks a quote char only if it both appears and correctly
// brackets fields (field starts with quote, and a matching close exists).
func detectQuote(sample []byte, delim byte) byte {
	for _, q := range quoteCandidates {
		if !bytes.Contains(sample, []byte{q}) {
			continue
		}
		// Heuristic: quote appears at a field start (after delimiter or
		// line start) at least twice, or once with an embedded delimiter
		// inside quotes — strong evidence of real quoting.
		starts := 0
		embedded := false
		in := false
		prev := byte(0)
		for i := 0; i < len(sample); i++ {
			ch := sample[i]
			if in {
				if ch == q {
					if i+1 < len(sample) && sample[i+1] == q {
						i++
						continue
					}
					in = false
				}
				if ch == delim {
					embedded = true
				}
				continue
			}
			if ch == q && (i == 0 || prev == delim || prev == '\n' || prev == '\r') {
				in = true
				starts++
			}
			prev = ch
		}
		if starts >= 2 || (starts >= 1 && embedded) {
			return q
		}
	}
	return 0
}

// detectHeader reports whether the first line looks like a header:
// same field count as the modal count, and at least one later line has
// a field that parses as a number where the header's same column does
// not. If evidence is inconclusive it defaults to false (no guessing).
func detectHeader(lines [][]byte, delim byte) bool {
	var rows [][]string
	for _, ln := range lines {
		if len(bytes.TrimSpace(ln)) == 0 {
			continue
		}
		rows = append(rows, splitFieldsSimple(ln, delim))
	}
	if len(rows) < 2 {
		return false
	}
	width := len(rows[0])
	for _, r := range rows[1:] {
		if len(r) != width {
			return false
		}
	}
	numericCol := false
	for c := 0; c < width; c++ {
		if looksNumeric(rows[0][c]) {
			continue
		}
		for _, r := range rows[1:] {
			if looksNumeric(r[c]) {
				numericCol = true
				break
			}
		}
	}
	return numericCol
}

func splitFieldsSimple(line []byte, delim byte) []string {
	parts := bytes.Split(line, []byte{delim})
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = string(bytes.TrimSpace(p))
	}
	return out
}

func looksNumeric(s string) bool {
	if s == "" {
		return false
	}
	digits := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			digits++
		case c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E':
		default:
			return false
		}
	}
	return digits > 0
}
