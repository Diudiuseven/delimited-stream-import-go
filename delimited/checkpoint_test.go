package delimited

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// idSink collects record IDs and can fail once after a threshold.
type idSink struct {
	mu     sync.Mutex
	ids    map[string]bool
	order  []string
	failAt int
	count  int
}

func (s *idSink) Write(ctx context.Context, records []Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range records {
		s.count++
		if s.failAt > 0 && s.count > s.failAt {
			return errors.New("sink failure")
		}
		id := r.Fields[0]
		if s.ids == nil {
			s.ids = make(map[string]bool)
		}
		if !s.ids[id] {
			s.ids[id] = true
			s.order = append(s.order, id)
		}
	}
	return nil
}

func (s *idSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.order))
	copy(out, s.order)
	return out
}

func resumeInput(n int) string {
	var sb strings.Builder
	sb.WriteString("id,name\n")
	for i := 0; i < n; i++ {
		sb.WriteString(fmt.Sprintf("%d,user%d\n", i, i))
	}
	return sb.String()
}

func TestCheckpointStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store := NewFileCheckpointStore(filepath.Join(dir, "cp.json"))
	cp := &Checkpoint{
		Format:      DefaultFormat(),
		ShardCount:  4,
		ShardIndex:  2,
		Offset:      12345,
		RecordIndex: 99,
		SourceSize:  54321,
	}
	if err := store.Save(cp); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("checkpoint not found")
	}
	if loaded.Offset != 12345 || loaded.RecordIndex != 99 || loaded.ShardIndex != 2 {
		t.Fatalf("loaded checkpoint mismatch: %+v", loaded)
	}
	// Overwrite.
	cp.Offset = 99999
	if err := store.Save(cp); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Offset != 99999 {
		t.Fatalf("overwrite failed: %+v", loaded)
	}
}

func TestCheckpointMissing(t *testing.T) {
	dir := t.TempDir()
	store := NewFileCheckpointStore(filepath.Join(dir, "missing.json"))
	cp, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cp != nil {
		t.Fatal("expected nil checkpoint for missing file")
	}
}

func TestImporterResumeAfterFailure(t *testing.T) {
	input := resumeInput(100)
	src := NewBytesSource([]byte(input))
	dir := t.TempDir()
	store := NewFileCheckpointStore(filepath.Join(dir, "cp.json"))

	sink := &idSink{failAt: 30}
	im, err := NewImporter(src, Config{ShardCount: 4, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	im.SetCheckpointStore(store)
	if err := im.Run(context.Background()); err == nil {
		t.Fatal("expected sink failure")
	}

	// Resume with the same sink (now succeeding).
	sink.failAt = 0
	src2 := NewBytesSource([]byte(input))
	im2, err := NewImporter(src2, Config{ShardCount: 4, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	im2.SetCheckpointStore(store)
	if err := im2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	ids := sink.snapshot()
	if len(ids) != 100 {
		t.Fatalf("got %d unique records, want 100", len(ids))
	}
	for i := 0; i < 100; i++ {
		if ids[i] != fmt.Sprintf("%d", i) {
			t.Fatalf("record %d = %q, want %q", i, ids[i], fmt.Sprintf("%d", i))
		}
	}
}

func TestImporterResumeAfterCancel(t *testing.T) {
	input := resumeInput(500)
	src := NewBytesSource([]byte(input))
	dir := t.TempDir()
	store := NewFileCheckpointStore(filepath.Join(dir, "cp.json"))

	sink := &idSink{}
	im, err := NewImporter(src, Config{ShardCount: 4, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	im.SetCheckpointStore(store)
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay.
	go func() {
		cancel()
	}()
	err = im.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}

	// Resume.
	src2 := NewBytesSource([]byte(input))
	im2, err := NewImporter(src2, Config{ShardCount: 4, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	im2.SetCheckpointStore(store)
	if err := im2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	ids := sink.snapshot()
	if len(ids) != 500 {
		t.Fatalf("got %d unique records, want 500", len(ids))
	}
}

func TestImporterResumeNoCheckpoint(t *testing.T) {
	// Without a store, a failed import re-commits everything (at-least-once).
	input := resumeInput(50)
	src := NewBytesSource([]byte(input))
	sink := &idSink{failAt: 20}
	im, err := NewImporter(src, Config{ShardCount: 2, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im.Run(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	sink.failAt = 0
	src2 := NewBytesSource([]byte(input))
	im2, err := NewImporter(src2, Config{ShardCount: 2, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := im2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Without checkpoint, records may be duplicated; just verify no data loss.
	ids := sink.snapshot()
	if len(ids) < 50 {
		t.Fatalf("lost records: got %d unique, want 50", len(ids))
	}
}

func TestImporterSourceChanged(t *testing.T) {
	input := resumeInput(50)
	src := NewBytesSource([]byte(input))
	dir := t.TempDir()
	store := NewFileCheckpointStore(filepath.Join(dir, "cp.json"))

	sink := &idSink{failAt: 10}
	im, err := NewImporter(src, Config{ShardCount: 2, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	im.SetCheckpointStore(store)
	if err := im.Run(context.Background()); err == nil {
		t.Fatal("expected failure")
	}

	// Resume with a different source.
	src2 := NewBytesSource([]byte(resumeInput(60)))
	im2, err := NewImporter(src2, Config{ShardCount: 2, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	im2.SetCheckpointStore(store)
	err = im2.Run(context.Background())
	var sc *ErrSourceChanged
	if !errors.As(err, &sc) {
		t.Fatalf("got %v, want ErrSourceChanged", err)
	}
}

func TestImporterResumeFromFile(t *testing.T) {
	// End-to-end: write a real file, import, resume.
	input := resumeInput(200)
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(path, []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	cpPath := filepath.Join(dir, "cp.json")
	store := NewFileCheckpointStore(cpPath)
	sink := &idSink{failAt: 40}
	im, err := NewImporter(src, Config{ShardCount: 4, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	im.SetCheckpointStore(store)
	if err := im.Run(context.Background()); err == nil {
		t.Fatal("expected failure")
	}

	sink.failAt = 0
	src2, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src2.Close()
	im2, err := NewImporter(src2, Config{ShardCount: 4, BatchSize: 1}, sink)
	if err != nil {
		t.Fatal(err)
	}
	im2.SetCheckpointStore(store)
	if err := im2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	ids := sink.snapshot()
	if len(ids) != 200 {
		t.Fatalf("got %d unique records, want 200", len(ids))
	}
}
