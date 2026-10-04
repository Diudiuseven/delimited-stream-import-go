// Package delimited implements a streaming, resumable, concurrent importer
// for large delimited text files (CSV, TSV and similar dialects).
//
// # Streaming
//
// The Parser is a byte-oriented state machine that reads one record at a
// time from an io.Reader. Memory usage is bounded by the configured maximum
// record size (MaxRecordBytes, default 1 MiB) plus a small read buffer; it
// never materializes the whole input. Records larger than the limit produce a
// recoverable ErrRecordTooLong: the parser skips the remainder of the record
// and continues with the next one.
//
// # Format detection
//
// When no explicit Format is supplied, the dialect (delimiter, quote,
// escape, line ending) is detected from an input sample. Detection is
// deterministic: the same sample always yields the same result. If the
// dialect cannot be determined with confidence (e.g. single-column data), an
// ErrUncertainFormat is returned; callers must supply an explicit Format
// override rather than relying on a silent guess.
//
// # Quoting
//
// Quoted fields may contain the delimiter, newlines and escaped quotes.
// Malformed quotes and unclosed fields produce errors carrying the byte
// offset and line number of the problem. In lenient mode (Strict=false) bad
// records are skipped and counted; in strict mode the stream aborts.
//
// # Headers and column mapping
//
// The first record is consumed as a header row by default and used for
// column name mapping. Records with a different field count are handled
// according to OnMismatch: reject (strict default), pad short records and
// drop extra fields, or drop mismatched records. All adjustments are counted
// in Stats; data is never silently lost.
//
// # Sharded concurrent processing
//
// A seekable Source can be split into record-aligned shards processed
// concurrently. Shard boundaries are computed with a single pass that tracks
// quote state, so records spanning newlines are never split. Results are
// merged in input order and committed through a Sink. Progress and error
// counts are queryable via Stats.
//
// # Checkpointing and resume
//
// A CheckpointStore records the commit position (absolute byte offset plus
// record count) atomically: the checkpoint is written to a temp file, fsynced
// and renamed, so a crash never leaves a half-written state. On resume the
// importer continues from the last committed record boundary. The source size
// is checked on resume; a changed source produces ErrSourceChanged.
//
// Commits are at-least-once: a crash between a sink commit and the
// checkpoint save may re-commit the last batch. For exactly-once semantics
// use BatchSize=1 (checkpoint per record) or an idempotent sink keyed by
// record index. The library guarantees it never skips records and never
// leaves its own state half-written.
package delimited
