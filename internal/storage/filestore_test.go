package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
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

func TestGetDeleteList(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("distributed storage test")
	id := "object1-0"

	err = store.Put(
		id,
		bytes.NewReader(data),
		int64(len(data)),
		sha256.Sum256(data),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Verify worker inventory.
	chunks, err := store.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(chunks) != 1 ||
		chunks[0].ID != id ||
		chunks[0].Size != int64(len(data)) {
		t.Fatalf("unexpected inventory: %+v", chunks)
	}

	// Open the chunk.
	reader, err := store.Get(id)
	if err != nil {
		t.Fatal(err)
	}

	// Delete while the file descriptor is still open.
	if err := store.Delete(id); err != nil {
		reader.Close()
		t.Fatal(err)
	}

	// Linux allows existing file descriptors to keep reading.
	got, err := io.ReadAll(reader)
	reader.Close()

	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("got %q, want %q", got, data)
	}

	// A new Get must now fail.
	_, err = store.Get(id)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not-exist error, got %v", err)
	}

	// The inventory should now be empty.
	chunks, err = store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("expected empty inventory: %+v", chunks)
	}
}

func TestConcurrentPut(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("same chunk contents")
	hash := sha256.Sum256(data)

	const writers = 20

	var wg sync.WaitGroup
	results := make(chan error, writers)

	for i := 0; i < writers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			err := store.Put(
				"shared-0",
				bytes.NewReader(data),
				int64(len(data)),
				hash,
			)

			results <- err
		}()
	}

	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Fatalf("concurrent Put failed: %v", err)
		}
	}

	chunks, err := store.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
}
