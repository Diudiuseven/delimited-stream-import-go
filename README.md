# delimited

A streaming, resumable, concurrent importer for large delimited text files
(CSV, TSV and similar dialects) in Go.

## Why

The usual approach reads the whole input into memory, then parses it. That
fails on files larger than RAM, aborts the whole batch when a single record
is malformed, and restarts from scratch after an interruption. This library
parses one record at a time, skips bad records in lenient mode, and resumes
from the last committed position.

## Features

- **Streaming**: a byte-oriented state machine parses one record at a time.
  Memory usage is bounded by `MaxRecordBytes` (default 1 MiB) plus a small
  read buffer; it never materializes the whole input.
- **Automatic format detection**: delimiter, quote character, escape style and
  line ending are detected from an input sample. Detection is deterministic:
  the same sample always yields the same result. When the dialect cannot be
  determined with confidence (e.g. single-column data), an
  `ErrUncertainFormat` is returned; supply an explicit `Format` override
  rather than relying on a silent guess.
- **Correct quoting**: quoted fields may contain the delimiter, newlines and
  escaped quotes (`""` or `\"`). Malformed quotes and unclosed fields produce
  errors carrying the byte offset and line number. Lenient mode skips the
  offending record and continues; strict mode aborts.
- **BOM and mixed newlines**: a leading UTF-8 BOM is stripped and never enters
  field content. LF, CRLF and CR line endings are accepted in any mix.
- **Header mapping and column validation**: the first record is consumed as a
  header row by default. Records with a different field count are handled per
  `OnMismatch`: reject (strict default), pad short records / drop extra
  fields, or drop mismatched records. All adjustments are counted in `Stats`;
  data is never silently lost.
- **Sharded concurrent processing**: a seekable `Source` is split into
  record-aligned shards processed concurrently. Shard boundaries are computed
  with a single quote-aware pass, so records spanning newlines are never
  split. Results are merged in input order. Progress and error counts are
  queryable via `Stats`.
- **Checkpointing and resume**: a `CheckpointStore` records the commit
  position (absolute byte offset plus record count) atomically — temp file,
  fsync, rename — so a crash never leaves a half-written state. On resume the
  importer continues from the last committed record boundary. The source size
  is checked on resume; a changed source produces `ErrSourceChanged`.

## Installation

```sh
go get github.com/Diudiuseven/delimited-stream-import-go
```

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/Diudiuseven/delimited-stream-import-go/delimited"
)

func main() {
    src, err := delimited.OpenFile("data.csv")
    if err != nil {
        panic(err)
    }
    defer src.Close()

    sink := delimited.SinkFunc(func(ctx context.Context, records []delimited.Record) error {
        for _, r := range records {
            fmt.Println(r.Fields)
        }
        return nil
    })

    cfg := delimited.Config{
        // Format is auto-detected when nil.
        // Strict: true aborts on malformed records; false skips them.
        Strict: false,
        // OnMismatch: MismatchReject (default), MismatchPad, MismatchDrop.
        OnMismatch: delimited.MismatchPad,
        // MaxRecordBytes bounds memory; oversize records are recoverable.
        MaxRecordBytes: 1 << 20,
        // ShardCount: 0 uses runtime.GOMAXPROCS.
        ShardCount: 4,
        // BatchSize: records per sink Write call.
        BatchSize: 128,
    }

    im, err := delimited.NewImporter(src, cfg, sink)
    if err != nil {
        panic(err)
    }
    // Enable resume:
    // im.SetCheckpointStore(delimited.NewFileCheckpointStore("checkpoint.json"))

    if err := im.Run(context.Background()); err != nil {
        panic(err)
    }
    fmt.Printf("committed=%d skipped=%d errors=%d\n",
        im.Stats().RecordsCommitted, im.Stats().RecordsSkipped, im.Stats().Errors)
}
```

## Streaming from a non-seekable reader

For pipes, network streams or compressed readers, use `StreamImporter`. It
does not support sharding or resume, but it is fully streaming:

```go
im, err := delimited.NewStreamImporter(reader, cfg, sink)
if err != nil {
    panic(err)
}
if err := im.Run(ctx); err != nil {
    panic(err)
}
```

## Explicit format override

```go
f := delimited.Format{
    Delimiter:  ';',
    Quote:      '"',
    Escape:     delimited.EscapeDoubleQuote,
    LineEnding: delimited.LineEndingAuto,
    Confidence: 1,
}
cfg := delimited.Config{Format: &f}
```

## Detection rules

Detection reads the first `SampleSize` bytes (default 64 KiB) and scores
candidates deterministically:

- **Line ending**: counts CRLF, CR and LF; the most frequent wins (ties → LF).
- **Quote**: `"` and `'` are scored by how often they open or close a field;
  the highest score wins (ties → `"`). A score of zero means quoting is
  absent.
- **Escape**: doubled quotes (`""`) and backslash-escaped quotes (`\"`) are
  counted; the more frequent style wins (ties → doubled quotes).
- **Delimiter**: candidates `,`, `\t`, `;`, `|`, `:` are scored by
  field-count consistency. The candidate with the most records sharing the
  modal field count wins; ties are broken by the priority order above. A
  delimiter that does not appear in the sample is not a candidate.

Detection is uncertain when no records are found or the modal field count is
less than 2 (single-column data). Data inconsistencies (varying field counts)
are *not* format uncertainty — they are handled by schema validation.

## Memory bounds

- The parser keeps a 64 KiB read buffer plus the current record's field bytes.
- `MaxRecordBytes` (default 1 MiB) caps the record size. When exceeded, the
  parser returns `ErrRecordTooLong` carrying the record's start position,
  skips the remainder of the record (quote-aware), and continues with the
  next record. The error is recoverable.
- Shard channels are bounded (256 records per shard), so memory does not grow
  with input length.

## Recovery semantics

- **Lenient mode** (`Strict: false`): malformed records, unclosed fields and
  column mismatches under `MismatchDrop` are skipped and counted in `Stats`.
  The `OnError` callback is invoked for each skipped record.
- **Strict mode** (`Strict: true`): any parse error or column mismatch aborts
  the stream with the position-bearing error.
- **Checkpointing**: after each successful sink commit, the checkpoint is
  written atomically. On resume, the importer continues from the last
  committed record boundary — no records are re-read or re-committed.
- **At-least-once commits**: a crash between a sink commit and the checkpoint
  save may re-commit the last batch. For exactly-once semantics use
  `BatchSize: 1` (checkpoint per record) or an idempotent sink keyed by the
  global `Record.Index`. The library guarantees it never skips records and
  never leaves its own state half-written.
- **Source change detection**: on resume, the current source size is compared
  to the checkpoint; a mismatch produces `ErrSourceChanged`.

## Error types

All errors carry position information where applicable:

| Error | Meaning | Recoverable |
| --- | --- | --- |
| `ErrRecordTooLong` | Record exceeds `MaxRecordBytes` | Yes |
| `ErrMalformedQuote` | Quote in an invalid position | Yes (lenient) |
| `ErrUnclosedQuote` | Input ends inside a quoted field | Terminal |
| `ErrColumnMismatch` | Field count != schema column count | Per policy |
| `ErrDuplicateHeader` | Header name appears twice | No |
| `ErrUncertainFormat` | Format detection is not confident | No (override) |
| `ErrUnsupportedEncoding` | UTF-16 BOM detected | No |
| `ErrSourceChanged` | Source size changed since checkpoint | No |
| `ErrCheckpointCorrupt` | Checkpoint file cannot be decoded | No |

## License

MIT
