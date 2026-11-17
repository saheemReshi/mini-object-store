package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPut(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello, distributed storage")
	digest := sha256.Sum256(data)
	id := "object123-0"

	// Test 1: Normal upload
	err = store.Put(
		id,
		bytes.NewReader(data),
		int64(len(data)),
		digest,
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(store.rootDir, id))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}

	// Test 2: Identical retry should succeed
	err = store.Put(
		id,
		bytes.NewReader(data),
		int64(len(data)),
		digest,
	)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}

	// Test 3: Different data with same ID must fail
	other := []byte("different chunk")
	err = store.Put(
		id,
		bytes.NewReader(other),
		int64(len(other)),
		sha256.Sum256(other),
	)

	if !errors.Is(err, ErrChunkConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	// Test 4: Invalid checksum must not publish a chunk
	badHash := sha256.Sum256([]byte("wrong content"))
	err = store.Put(
		"newchunk-0",
		bytes.NewReader(data),
		int64(len(data)),
		badHash,
	)

	if !errors.Is(err, ErrInvalidChunk) {
		t.Fatalf("expected invalid chunk, got %v", err)
	}

	if _, err := os.Stat(filepath.Join(store.rootDir, "newchunk-0")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid chunk was published: %v", err)
	}
}
