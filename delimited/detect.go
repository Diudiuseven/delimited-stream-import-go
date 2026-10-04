package delimited

import (
	"bytes"
	"fmt"
	"io"
	"sort"
)

// DetectFormat reads up to sampleSize bytes from r, detects the delimited
// dialect and returns a Format. r is seeked back to its starting position
// before returning, so detection is non-destructive. Detection is
// deterministic: the same sample always yields the same result.
func DetectFormat(r io.ReadSeeker, sampleSize int) (*Format, error) {
	if sampleSize <= 0 {
		sampleSize = 64 * 1024
	}
	sample := make([]byte, sampleSize)
	n, err := io.ReadFull(r, sample)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, err
	}
	sample = sample[:n]
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return DetectFormatBytes(sample)
}

// DetectFormatBytes detects the dialect of a delimited sample. The result is
// deterministic for the same input bytes.
func DetectFormatBytes(sample []byte) (*Format, error) {
	f := DefaultFormat()
	var reasons []string

	// Strip BOM / detect unsupported encodings.
	if len(sample) >= 2 && sample[0] == 0xFF && sample[1] == 0xFE {
		return nil, &ErrUnsupportedEncoding{Encoding: "UTF-16LE"}
	}
	if len(sample) >= 2 && sample[0] == 0xFE && sample[1] == 0xFF {
		return nil, &ErrUnsupportedEncoding{Encoding: "UTF-16BE"}
	}
	if len(sample) >= 3 && sample[0] == 0xEF && sample[1] == 0xBB && sample[2] == 0xBF {
		sample = sample[3:]
	}
	if len(sample) == 0 {
		f.Uncertain = true
		f.Reasons = []string{"empty sample"}
		return &f, nil
	}

	// 1. Line endings.
	crlf, cr, lf := countLineEndings(sample)
	switch {
	case crlf >= lf && crlf >= cr && crlf > 0:
		f.LineEnding = LineEndingCRLF
	case cr > lf && cr > 0:
		f.LineEnding = LineEndingCR
	default:
		f.LineEnding = LineEndingLF
	}

	// 2. Quote character.
	quote, quoteScore := detectQuote(sample)
	f.Quote = quote
	if quote == 0 {
		reasons = append(reasons, "no quote character detected")
	}

	// 3. Escape style.
	if quote != 0 {
		f.Escape = detectEscape(sample, quote)
	}

	// 4. Delimiter.
	delim, _, modalCount, recordCount := detectDelim(sample, quote, f.Escape)
	f.Delimiter = delim

	// Confidence / uncertainty. Uncertainty is about the dialect (delimiter,
	// quote, escape), not about data consistency: varying field counts are a
	// data issue handled by schema validation, not a format issue.
	switch {
	case recordCount == 0:
		f.Uncertain = true
		reasons = append(reasons, "no records found in sample")
	case modalCount < 2:
		f.Uncertain = true
		reasons = append(reasons, fmt.Sprintf("most records have %d field(s); delimiter unclear", modalCount))
	}
	if quote == 0 && quoteScore == 0 {
		// Not uncertain per se; quoting is simply absent.
	}
	if f.Uncertain {
		f.Confidence = 0
		f.Reasons = reasons
		return &f, nil
	}
	f.Confidence = 1
	f.Reasons = reasons
	return &f, nil
}

// countLineEndings counts CRLF, CR and LF occurrences. CRLF is counted once.
func countLineEndings(sample []byte) (crlf, cr, lf int) {
	for i := 0; i < len(sample); i++ {
		switch sample[i] {
		case '\n':
			lf++
		case '\r':
			if i+1 < len(sample) && sample[i+1] == '\n' {
				crlf++
				i++
			} else {
				cr++
			}
		}
	}
	return
}

// detectQuote scores candidate quote characters and returns the best one.
// A quote is well-placed when it opens a field (at record start, after a
// delimiter, or after leading whitespace) or closes a field (followed by a
// delimiter, line ending, EOF, or trailing whitespace).
func detectQuote(sample []byte) (rune, int) {
	candidates := []byte{'"', '\''}
	best := byte(0)
	bestScore := 0
	for _, q := range candidates {
		score := scoreQuote(sample, q)
		if score > bestScore {
			bestScore = score
			best = q
		}
	}
	if best == 0 {
		return 0, 0
	}
	return rune(best), bestScore
}

func scoreQuote(sample []byte, q byte) int {
	score := 0
	inQuotes := false
	atFieldStart := true
	for i := 0; i < len(sample); i++ {
		b := sample[i]
		switch {
		case inQuotes:
			if b == '\\' && i+1 < len(sample) && sample[i+1] == q {
				// backslash-escaped quote: not a closing quote
				i++
				continue
			}
			if b == q {
				// could be escaped or closing
				if i+1 < len(sample) && sample[i+1] == q {
					score++ // escaped quote
					i++
					continue
				}
				// closing: next non-space should be delimiter/EOL/EOF
				j := i + 1
				for j < len(sample) && (sample[j] == ' ' || sample[j] == '\t') {
					j++
				}
				if j >= len(sample) || isLineEnd(sample[j]) || sample[j] == ',' || sample[j] == '\t' || sample[j] == ';' || sample[j] == '|' || sample[j] == ':' {
					score++
				} else {
					score -= 2
				}
				inQuotes = false
				atFieldStart = false
			}
		case b == q:
			if atFieldStart {
				score++
				inQuotes = true
			} else {
				score -= 2
			}
		case b == ' ' || b == '\t':
			// whitespace
		case isLineEnd(b):
			atFieldStart = true
		case b == ',' || b == '\t' || b == ';' || b == '|' || b == ':':
			atFieldStart = true
		default:
			atFieldStart = false
		}
	}
	return score
}

func isLineEnd(b byte) bool { return b == '\n' || b == '\r' }

// detectEscape chooses between double-quote escaping and backslash escaping.
func detectEscape(sample []byte, quote rune) EscapeStyle {
	q := byte(quote)
	doubled := 0
	backslashed := 0
	for i := 0; i < len(sample); i++ {
		if sample[i] == q && i+1 < len(sample) && sample[i+1] == q {
			doubled++
			i++
			continue
		}
		if sample[i] == '\\' && i+1 < len(sample) && sample[i+1] == q {
			backslashed++
			i++
		}
	}
	if backslashed > doubled {
		return EscapeBackslash
	}
	return EscapeDoubleQuote
}

// detectDelim scores candidate delimiters by field-count consistency.
func detectDelim(sample []byte, quote rune, escape EscapeStyle) (delim rune, score, modalCount, recordCount int) {
	candidates := []rune{',', '\t', ';', '|', ':'}
	type result struct {
		delim       rune
		score       int
		modal       int
		recordCount int
	}
	var results []result
	for _, d := range candidates {
		if !bytes.ContainsRune(sample, d) {
			continue
		}
		counts := fieldCounts(sample, d, quote, escape)
		if len(counts) == 0 {
			continue
		}
		modal := mostCommon(counts)
		s := 0
		for _, c := range counts {
			if c == modal {
				s++
			} else {
				s -= 2
			}
		}
		results = append(results, result{d, s, modal, len(counts)})
	}
	// Sort by score descending, tie broken by candidate priority order.
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].score != results[j].score {
			return results[i].score > results[j].score
		}
		return priority(results[i].delim) < priority(results[j].delim)
	})
	if len(results) == 0 {
		return ',', 0, 0, 0
	}
	best := results[0]
	return best.delim, best.score, best.modal, best.recordCount
}

func priority(delim rune) int {
	switch delim {
	case ',':
		return 0
	case '\t':
		return 1
	case ';':
		return 2
	case '|':
		return 3
	case ':':
		return 4
	}
	return 5
}

// mostCommon returns the most common value in counts.
func mostCommon(counts []int) int {
	freq := make(map[int]int)
	for _, c := range counts {
		freq[c]++
	}
	best := 0
	bestFreq := 0
	// deterministic iteration: iterate over a bounded range; on ties prefer
	// the larger field count (more likely to be the real delimiter).
	for c := 1; c <= 1000; c++ {
		if freq[c] > bestFreq || (freq[c] == bestFreq && c > best) {
			bestFreq = freq[c]
			best = c
		}
	}
	return best
}

// fieldCounts returns the number of fields per record, parsing the sample
// with a quote-aware state machine.
func fieldCounts(sample []byte, delim rune, quote rune, escape EscapeStyle) []int {
	var counts []int
	fields := 1
	inQuotes := false
	atFieldStart := true
	started := false // any byte seen in this record
	for i := 0; i < len(sample); i++ {
		b := sample[i]
		switch {
		case inQuotes:
			started = true
			if b == '\\' && escape == EscapeBackslash && i+1 < len(sample) && (sample[i+1] == byte(quote) || sample[i+1] == '\\') {
				i++
				continue
			}
			if quote != 0 && b == byte(quote) {
				if i+1 < len(sample) && sample[i+1] == byte(quote) && escape == EscapeDoubleQuote {
					i++
					continue
				}
				inQuotes = false
			}
		case quote != 0 && b == byte(quote) && atFieldStart:
			inQuotes = true
			atFieldStart = false
			started = true
		case rune(b) == delim:
			fields++
			atFieldStart = true
			started = true
		case isLineEnd(b):
			if b == '\r' && i+1 < len(sample) && sample[i+1] == '\n' {
				i++
			}
			if started {
				counts = append(counts, fields)
			}
			fields = 1
			inQuotes = false
			atFieldStart = true
			started = false
		default:
			if b != ' ' && b != '\t' {
				atFieldStart = false
			}
			started = true
		}
	}
	if started {
		counts = append(counts, fields)
	}
	return counts
}

// FormatCandidate is a detected dialect candidate with its score.
type FormatCandidate struct {
	Format Format
	Score  int
}

// String returns a debug representation of the format.
func (f Format) String() string {
	return fmt.Sprintf("Format(delimiter=%q quote=%q escape=%s lineEnding=%s confidence=%.2f uncertain=%v)",
		f.Delimiter, f.Quote, f.Escape, f.LineEnding, f.Confidence, f.Uncertain)
}

var _ = bytes.MinRead
