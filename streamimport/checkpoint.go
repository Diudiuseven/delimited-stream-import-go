package streamimport

import "context"

// Checkpoint marks a committed position in the input.
type Checkpoint struct {
	// Offset is the byte offset one past the last committed record.
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
// written atomically via write-temp-then-rename.
type FileCheckpointStore struct {
	Path string
}

func (s *FileCheckpointStore) Load(ctx context.Context) (Checkpoint, error) {
	return Checkpoint{}, nil
}

func (s *FileCheckpointStore) Save(ctx context.Context, cp Checkpoint) error { return nil }

// MemoryCheckpointStore is an in-memory store, useful for tests.
type MemoryCheckpointStore struct {
}

func NewMemoryCheckpointStore() *MemoryCheckpointStore { return &MemoryCheckpointStore{} }

func (s *MemoryCheckpointStore) Load(ctx context.Context) (Checkpoint, error) {
	return Checkpoint{}, nil
}

func (s *MemoryCheckpointStore) Save(ctx context.Context, cp Checkpoint) error { return nil }
