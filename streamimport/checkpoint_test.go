package streamimport

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFileCheckpointStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}

	cp, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cp != (Checkpoint{}) {
		t.Fatalf("missing file should yield zero checkpoint, got %+v", cp)
	}

	want := Checkpoint{Offset: 1234, Records: 56}
	if err := s.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	// Temp file must not linger.
	if _, err := os.Stat(s.Path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
}

func TestFileCheckpointStoreCorrupt(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "cp.json")
	if err := os.WriteFile(p, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &FileCheckpointStore{Path: p}
	if _, err := s.Load(context.Background()); err == nil {
		t.Fatal("corrupt checkpoint must error, not silently reset")
	}
}

func TestFileCheckpointStoreConcurrent(t *testing.T) {
	dir := t.TempDir()
	s := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := s.Save(context.Background(), Checkpoint{Offset: int64(n * j), Records: int64(j)}); err != nil {
					t.Error(err)
					return
				}
				if _, err := s.Load(context.Background()); err != nil {
					t.Error(err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if _, err := s.Load(context.Background()); err != nil {
		t.Fatalf("final load after concurrent saves: %v", err)
	}
}
