package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const ChunkSize int64 = 1_000_000

var (
	ErrInvalidChunk  = errors.New("invalid chunk")
	ErrChunkConflict = errors.New("chunk already exists with different contents")
)

type ChunkInfo struct {
	ID   string `json:"chunk_id"`
	Size int64  `json:"size"`
}

type FileStore struct {
	rootDir string

	locksMu    sync.Mutex
	chunkLocks map[string]*sync.RWMutex
}

func NewFileStore(rootDir string) (*FileStore, error) {
	if err := os.MkdirAll(rootDir, 0700); err != nil {
		return nil, fmt.Errorf(
			"create storage directory: %w", err,
		)
	}

	return &FileStore{
		rootDir:    rootDir,
		chunkLocks: make(map[string]*sync.RWMutex),
	}, nil
}

func (s *FileStore) lockFor(id string) *sync.RWMutex {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()

	if mu, exists := s.chunkLocks[id]; exists {
		return mu
	}

	mu := &sync.RWMutex{}
	s.chunkLocks[id] = mu

	return mu
}

func (s *FileStore) List() ([]ChunkInfo, error) {
	entries, err := os.ReadDir(s.rootDir)
	if err != nil {
		return nil, fmt.Errorf("list chunks: %w", err)
	}

	var chunks []ChunkInfo

	for _, entry := range entries {
		id := entry.Name()

		if !validChunkID(id) {
			continue
		}

		chunkMu := s.lockFor(id)
		chunkMu.RLock()

		info, err := os.Lstat(
			filepath.Join(s.rootDir, id),
		)

		chunkMu.RUnlock()

		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf(
				"stat chunk %s: %w", id, err,
			)
		}

		if !info.Mode().IsRegular() {
			continue
		}

		chunks = append(chunks, ChunkInfo{
			ID:   id,
			Size: info.Size(),
		})
	}

	return chunks, nil
}

func (s *FileStore) Put(
	id string,
	src io.Reader,
	expectedSize int64,
	expectedHash [32]byte,
) error {
	if !validChunkID(id) || expectedSize <= 0 || expectedSize > ChunkSize {
		return ErrInvalidChunk
	}

	// 1. Create a temporary file inside the storage directory.
	tmp, err := os.CreateTemp(s.rootDir, ".tmp-chunk-*")
	if err != nil {
		return fmt.Errorf("create temporary chunk: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	// 2. Stream incoming bytes to disk and compute their hash.
	hasher := sha256.New()

	n, err := io.Copy(
		io.MultiWriter(tmp, hasher),
		io.LimitReader(src, expectedSize+1),
	)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}

	// 3. Verify length and checksum.
	if n != expectedSize {
		return fmt.Errorf(
			"%w: expected %d bytes, got %d",
			ErrInvalidChunk, expectedSize, n,
		)
	}

	if !bytes.Equal(hasher.Sum(nil), expectedHash[:]) {
		return fmt.Errorf("%w: checksum mismatch", ErrInvalidChunk)
	}

	// 4. Flush data and close the temporary file.
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync chunk: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close chunk: %w", err)
	}

	// 5. Synchronize publication with Get, Delete and List.
	chunkMu := s.lockFor(id)

	chunkMu.Lock()
	defer chunkMu.Unlock()

	// Publish without overwriting an existing chunk.
	finalPath := filepath.Join(s.rootDir, id)

	if err := os.Link(tmp.Name(), finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			if err := verifyExisting(
				finalPath, expectedSize, expectedHash,
			); err != nil {
				return err
			}

			return syncDirectory(s.rootDir)
		}

		return fmt.Errorf("publish chunk: %w", err)
	}

	// 6. Remove temporary name and persist directory changes.
	if err := os.Remove(tmp.Name()); err != nil {
		return fmt.Errorf("remove temporary chunk name: %w", err)
	}

	if err := syncDirectory(s.rootDir); err != nil {
		return fmt.Errorf("sync storage directory: %w", err)
	}

	return nil
}

func (s *FileStore) Get(id string) (io.ReadCloser, error) {
	if !validChunkID(id) {
		return nil, ErrInvalidChunk
	}

	chunkMu := s.lockFor(id)

	chunkMu.RLock()
	defer chunkMu.RUnlock()

	path := filepath.Join(s.rootDir, id)

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open chunk %s: %w", id, err)
	}

	return f, nil
}

func (s *FileStore) Delete(id string) error {
	if !validChunkID(id) {
		return ErrInvalidChunk
	}
	chunkMu := s.lockFor(id)

	chunkMu.Lock()
	defer chunkMu.Unlock()

	path := filepath.Join(s.rootDir, id)

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("delete chunk %s: %w", id, err)
	}

	if err := syncDirectory(s.rootDir); err != nil {
		return fmt.Errorf("sync after deleting chunk: %w", err)
	}

	return nil
}

// Repeated PUTs are successful only when the stored bytes match.
func verifyExisting(
	path string,
	expectedSize int64,
	expectedHash [32]byte,
) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open existing chunk: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	n, err := io.Copy(hasher, f)
	if err != nil {
		return fmt.Errorf("read existing chunk: %w", err)
	}

	if n != expectedSize ||
		!bytes.Equal(hasher.Sum(nil), expectedHash[:]) {
		return ErrChunkConflict
	}

	return nil
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()

	return f.Sync()
}

// Restrict IDs so they cannot escape the storage directory.
func validChunkID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}

	for _, c := range id {
		if !(c >= 'a' && c <= 'z' ||
			c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' ||
			c == '-' || c == '_') {
			return false
		}
	}

	return true
}
