package delimited

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Checkpoint records the commit position of an import so it can resume
// without re-reading or re-committing completed records.
type Checkpoint struct {
	// Version is the checkpoint format version.
	Version int `json:"version"`
	// Format is the dialect used for the import. Detection is not re-run on
	// resume so results stay stable.
	Format Format `json:"format"`
	// ShardCount is the number of shards the input was split into.
	ShardCount int `json:"shard_count"`
	// ShardIndex is the shard currently being processed.
	ShardIndex int `json:"shard_index"`
	// Offset is the absolute byte offset up to which all records have been
	// committed. It always points at a record boundary.
	Offset int64 `json:"offset"`
	// RecordIndex is the number of records committed so far.
	RecordIndex int64 `json:"record_index"`
	// SourceSize is the size of the input when the checkpoint was written,
	// used to detect a changed source on resume.
	SourceSize int64 `json:"source_size"`
	// UpdatedAt is when the checkpoint was last written.
	UpdatedAt time.Time `json:"updated_at"`
}

// CheckpointStore persists checkpoints atomically.
type CheckpointStore interface {
	// Save atomically writes the checkpoint.
	Save(cp *Checkpoint) error
	// Load returns the last saved checkpoint. A missing checkpoint returns
	// (nil, nil).
	Load() (*Checkpoint, error)
}

// FileCheckpointStore writes checkpoints to a file using temp file + fsync +
// rename so a crash never leaves a half-written checkpoint.
type FileCheckpointStore struct {
	path string
}

// NewFileCheckpointStore stores checkpoints at the given path.
func NewFileCheckpointStore(path string) *FileCheckpointStore {
	return &FileCheckpointStore{path: path}
}

// Save atomically writes the checkpoint.
func (s *FileCheckpointStore) Save(cp *Checkpoint) error {
	if cp == nil {
		return fmt.Errorf("delimited: cannot save nil checkpoint")
	}
	cp.Version = 1
	cp.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".checkpoint-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck
	if _, err := tmp.Write(data); err != nil {
		tmp.Close() //nolint:errcheck
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close() //nolint:errcheck
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return err
	}
	// Fsync the directory so the rename itself is durable.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Load returns the last saved checkpoint. A missing file returns (nil, nil).
func (s *FileCheckpointStore) Load() (*Checkpoint, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, &ErrCheckpointCorrupt{Path: s.path, Err: err}
	}
	if cp.Version != 1 {
		return nil, &ErrCheckpointCorrupt{Path: s.path, Err: fmt.Errorf("unsupported version %d", cp.Version)}
	}
	return &cp, nil
}
