package streamimport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Checkpoint marks a committed position in the input.
type Checkpoint struct {
	// Offset is the byte offset one past the last committed record,
	// relative to the first byte after any BOM.
	Offset int64
	// Records is the number of records committed so far.
	Records int64
}

// CheckpointStore persists checkpoints. Implementations must be safe
// for concurrent use and writes must be atomic (no torn state).
type CheckpointStore interface {
	Load(ctx context.Context) (Checkpoint, error)
	Save(ctx context.Context, cp Checkpoint) error
}

// FileCheckpointStore stores a checkpoint as a small JSON file,
// written atomically via write-temp-then-rename, so a crash mid-save
// never leaves a half-written checkpoint.
type FileCheckpointStore struct {
	Path string
	mu   sync.Mutex
}

func (s *FileCheckpointStore) Load(ctx context.Context) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return Checkpoint{}, nil
	}
	if err != nil {
		return Checkpoint{}, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		return Checkpoint{}, fmt.Errorf("streamimport: corrupt checkpoint %s: %w", s.Path, err)
	}
	return cp, nil
}

func (s *FileCheckpointStore) Save(ctx context.Context, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	tmp := s.Path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	// fsync the temp file so the rename is durable.
	if f, err := os.OpenFile(tmp, os.O_RDWR, 0); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, s.Path); err != nil {
		return err
	}
	// fsync the directory so the rename itself is durable.
	if d, err := os.Open(filepath.Dir(s.Path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// MemoryCheckpointStore is an in-memory store, useful for tests.
type MemoryCheckpointStore struct {
	mu sync.Mutex
	cp Checkpoint
}

func NewMemoryCheckpointStore() *MemoryCheckpointStore { return &MemoryCheckpointStore{} }

func (s *MemoryCheckpointStore) Load(ctx context.Context) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cp, nil
}

func (s *MemoryCheckpointStore) Save(ctx context.Context, cp Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cp = cp
	return nil
}
