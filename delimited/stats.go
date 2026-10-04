package delimited

import "sync/atomic"

// Stats holds queryable progress and error counters. All counters are safe
// for concurrent reads; use Snapshot for a consistent point-in-time copy.
type Stats struct {
	// BytesRead is the number of input bytes consumed.
	BytesRead int64
	// RecordsRead is the number of records successfully parsed.
	RecordsRead int64
	// RecordsCommitted is the number of records handed to the sink.
	RecordsCommitted int64
	// RecordsSkipped is the number of records dropped in lenient mode
	// (malformed records or dropped columns).
	RecordsSkipped int64
	// ColumnsPadded is the number of fields added to short records.
	ColumnsPadded int64
	// ColumnsDropped is the number of fields removed from long records.
	ColumnsDropped int64
	// Errors is the total number of recoverable errors encountered.
	Errors int64
	// BlankLines is the number of empty lines skipped.
	BlankLines int64
	// ShardsCompleted is the number of shards that finished processing.
	ShardsCompleted int64
}

// Snapshot returns a copy of the stats with all counters read atomically.
func (s *Stats) Snapshot() Stats {
	return Stats{
		BytesRead:        atomic.LoadInt64(&s.BytesRead),
		RecordsRead:      atomic.LoadInt64(&s.RecordsRead),
		RecordsCommitted: atomic.LoadInt64(&s.RecordsCommitted),
		RecordsSkipped:   atomic.LoadInt64(&s.RecordsSkipped),
		ColumnsPadded:    atomic.LoadInt64(&s.ColumnsPadded),
		ColumnsDropped:   atomic.LoadInt64(&s.ColumnsDropped),
		Errors:           atomic.LoadInt64(&s.Errors),
		BlankLines:       atomic.LoadInt64(&s.BlankLines),
		ShardsCompleted:  atomic.LoadInt64(&s.ShardsCompleted),
	}
}

func (s *Stats) addBytes(n int64)      { atomic.AddInt64(&s.BytesRead, n) }
func (s *Stats) addRead()              { atomic.AddInt64(&s.RecordsRead, 1) }
func (s *Stats) addCommitted(n int64)  { atomic.AddInt64(&s.RecordsCommitted, n) }
func (s *Stats) addSkipped()           { atomic.AddInt64(&s.RecordsSkipped, 1) }
func (s *Stats) addPadded(n int64)     { atomic.AddInt64(&s.ColumnsPadded, n) }
func (s *Stats) addDropped(n int64)    { atomic.AddInt64(&s.ColumnsDropped, n) }
func (s *Stats) addError()             { atomic.AddInt64(&s.Errors, 1) }
func (s *Stats) addBlankLines(n int64) { atomic.AddInt64(&s.BlankLines, n) }
func (s *Stats) addShard()             { atomic.AddInt64(&s.ShardsCompleted, 1) }
