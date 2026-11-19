package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// ChunkSize is a decimal megabyte; the final object chunk may be smaller.
const ChunkSize int64 = 1_000_000

var (
	ErrInvalidChunk  = errors.New("invalid chunk")
	ErrChunkConflict = errors.New("chunk already exists with different contents")
)

type ChunkInfo struct {
	ID   string `json:"chunk_id"`
	Size int64  `json:"size"`
}
type chunkLock struct {
	mu   sync.RWMutex
	refs int
}

// FileStore keeps immutable chunk files in one trusted directory. One worker
// process must own each directory: these mutexes do not coordinate other processes.
type FileStore struct {
	rootDir string

	locksMu    sync.Mutex
	chunkLocks map[string]*chunkLock
}

func NewFileStore(rootDir string) (*FileStore, error) {
	if err := os.MkdirAll(rootDir, 0700); err != nil {
		return nil, fmt.Errorf(
			"create storage directory: %w", err,
		)
	}

	return &FileStore{
		rootDir:    rootDir,
		chunkLocks: make(map[string]*chunkLock),
	}, nil
}

func (s *FileStore) lockChunk(id string, write bool) func() {
	// Register both holders and waiters before acquiring the chunk mutex. This
	// keeps one shared lock alive until every operation using it has finished.
	s.locksMu.Lock()

	entry, exists := s.chunkLocks[id]
	if !exists {
		entry = &chunkLock{}
		s.chunkLocks[id] = entry
	}

	entry.refs++
	s.locksMu.Unlock()

	// Do not wait with locksMu held: current holders need it to release refs.
	if write {
		entry.mu.Lock()
	} else {
		entry.mu.RLock()
	}

	// Return a function that releases both
	// the chunk lock and its registry reference.
	return func() {
		if write {
			entry.mu.Unlock()
		} else {
			entry.mu.RUnlock()
		}

		s.locksMu.Lock()
		entry.refs--

		if entry.refs == 0 {
			delete(s.chunkLocks, id)
		}

		s.locksMu.Unlock()
	}
}

// List reports regular, published chunk files. Concurrent deletion can remove an
// entry during the scan, so this is an inventory view rather than an atomic snapshot.
func (s *FileStore) List() ([]ChunkInfo, error) {
	entries, err := os.ReadDir(s.rootDir)
	if err != nil {
		return nil, fmt.Errorf("list chunks: %w", err)
	}

	chunks := make([]ChunkInfo, 0)

	for _, entry := range entries {
		id := entry.Name()

		if !validChunkID(id) {
			continue
		}

		unlock := s.lockChunk(id, false)

		info, err := os.Lstat(
			filepath.Join(s.rootDir, id),
		)

		unlock()

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

// Put stores a verified immutable chunk without a cancellation deadline. Identical
// retries succeed; reusing an ID for different bytes returns ErrChunkConflict.
func (s *FileStore) Put(
	id string,
	src io.Reader,
	expectedSize int64,
	expectedHash [32]byte,
) error {
	return s.PutContext(context.Background(), id, src, expectedSize, expectedHash)
}

// PutContext serializes the entire write with deletion, including staging.
// Cancellation observed before publication leaves no chunk behind. Cancellation
// racing the publication syscall still requires controller-side cleanup.
func (s *FileStore) PutContext(ctx context.Context, id string, src io.Reader, expectedSize int64, expectedHash [32]byte) error {
	if !validChunkID(id) || expectedSize <= 0 || expectedSize > ChunkSize {
		return ErrInvalidChunk
	}
	unlock := s.lockChunk(id, true)
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	// Stage on the same filesystem as the final name so hard-link publication
	// is atomic; readers never open a partially written chunk.
	tmp, err := os.CreateTemp(s.rootDir, ".tmp-chunk-*")
	if err != nil {
		return fmt.Errorf("create temporary chunk: %w", err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	// Stream incoming bytes to disk and compute their hash.
	hasher := sha256.New()

	// Read one extra byte to distinguish an exact-size body from an oversized
	// one without trusting Content-Length or consuming an unbounded reader.
	n, err := io.Copy(
		io.MultiWriter(tmp, hasher),
		io.LimitReader(src, expectedSize+1),
	)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}

	// Verify length and checksum before the temporary file becomes visible.
	if n != expectedSize {
		return fmt.Errorf(
			"%w: expected %d bytes, got %d",
			ErrInvalidChunk, expectedSize, n,
		)
	}

	if !bytes.Equal(hasher.Sum(nil), expectedHash[:]) {
		return fmt.Errorf("%w: checksum mismatch", ErrInvalidChunk)
	}

	// Persist contents before publishing their name. The later directory sync
	// persists the name itself; syncing file data alone is not sufficient.
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync chunk: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close chunk: %w", err)
	}

	// Honor cancellation immediately before publication while still holding
	// the chunk write lock acquired before staging.
	if err := ctx.Err(); err != nil {
		return err
	}
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

	// Remove the temporary name and persist directory changes.
	if err := os.Remove(tmp.Name()); err != nil {
		return fmt.Errorf("remove temporary chunk name: %w", err)
	}

	if err := syncDirectory(s.rootDir); err != nil {
		return fmt.Errorf("sync storage directory: %w", err)
	}

	return nil
}

// Get opens a published chunk; the caller must close the returned descriptor.
// On Linux, unlink removes a filename but an already-open descriptor retains its
// inode. Immutable contents therefore remain readable after the lock is released
// even if Delete removes the name or another Put publishes a new inode for it.
func (s *FileStore) Get(id string) (io.ReadCloser, error) {
	if !validChunkID(id) {
		return nil, ErrInvalidChunk
	}

	unlock := s.lockChunk(id, false)
	defer unlock()

	path := filepath.Join(s.rootDir, id)

	f, err := openChunk(path)
	if err != nil {
		return nil, fmt.Errorf("open chunk %s: %w", id, err)
	}

	return f, nil
}

// Delete unlinks a chunk and syncs the directory. An absent name returns an error
// wrapping os.ErrNotExist; the controller client treats the resulting 404 as an
// acknowledged, idempotent deletion.
func (s *FileStore) Delete(id string) error {
	if !validChunkID(id) {
		return ErrInvalidChunk
	}
	unlock := s.lockChunk(id, true)
	defer unlock()

	path := filepath.Join(s.rootDir, id)

	if err := os.Remove(path); err != nil {
		// A previous unlink may have succeeded but failed its directory sync.
		// Retry the sync before acknowledging that the chunk is absent.
		if errors.Is(err, os.ErrNotExist) {
			if syncErr := syncDirectory(s.rootDir); syncErr != nil {
				return fmt.Errorf("sync absent chunk: %w", syncErr)
			}
		}
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
	f, err := openChunk(path)
	if err != nil {
		return fmt.Errorf("open existing chunk: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	n, err := io.Copy(hasher, io.LimitReader(f, expectedSize+1))
	if err != nil {
		return fmt.Errorf("read existing chunk: %w", err)
	}

	if n != expectedSize ||
		!bytes.Equal(hasher.Sum(nil), expectedHash[:]) {
		return ErrChunkConflict
	}

	return nil
}

// O_NOFOLLOW rejects symlinks; O_NONBLOCK prevents a malicious FIFO from
// blocking before Stat can reject it. The storage directory itself is trusted.
func openChunk(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrInvalidChunk
	}
	return f, nil
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
