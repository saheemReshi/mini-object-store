package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const ChunkSize int64 = 1_000_000

var (
	ErrInvalidChunk  = errors.New("invalid chunk")
	ErrChunkConflict = errors.New("chunk already exists with different contents")
)

type FileStore struct {
	rootDir string
}

func NewFileStore(rootDir string) (*FileStore, error) {
	if err := os.MkdirAll(rootDir, 0700); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	return &FileStore{rootDir: rootDir}, nil
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

	// 5. Publish without overwriting an existing chunk.
	finalPath := filepath.Join(s.rootDir, id)

	if err := os.Link(tmp.Name(), finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return verifyExisting(finalPath, expectedSize, expectedHash)
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
