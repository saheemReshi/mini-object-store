package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"mini-object-store/internal/storage"
	"mini-object-store/internal/worker"
)

func TestWorkerClient(t *testing.T) {
	dir := t.TempDir()

	store, err := storage.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Start an actual HTTP worker on a temporary port.
	server := httptest.NewServer(
		worker.NewHandler(store, "worker1"),
	)
	defer server.Close()

	client := NewWorkerClient()
	ctx := context.Background()

	data := []byte("hello distributed storage")
	hash := sha256.Sum256(data)
	id := "obj123-0"

	// 1. PUT
	err = client.PutChunk(ctx, server.URL, id, data, hash)
	if err != nil {
		t.Fatal(err)
	}

	// 2. GET and verify
	got, err := client.GetChunk(
		ctx, server.URL, id, int64(len(data)), hash,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match")
	}

	// 3. Repeated PUT should succeed.
	if err := client.PutChunk(ctx, server.URL, id, data, hash); err != nil {
		t.Fatal(err)
	}

	// 4. Different bytes with the same ID must conflict.
	other := []byte("different data")
	err = client.PutChunk(
		ctx, server.URL, id, other, sha256.Sum256(other),
	)
	if !errors.Is(err, ErrChunkConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	// 5. Inventory should contain our chunk.
	inventory, err := client.ListChunks(
		ctx, server.URL, "worker1",
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(inventory.Chunks) != 1 ||
		inventory.Chunks[0].ID != id {
		t.Fatalf("unexpected inventory: %+v", inventory)
	}

	// 6. Intentionally corrupt the stored chunk.
	err = os.WriteFile(
		filepath.Join(dir, id),
		[]byte("corrupted!"),
		0600,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.GetChunk(
		ctx, server.URL, id, int64(len(data)), hash,
	)
	if !errors.Is(err, ErrChunkCorrupt) {
		t.Fatalf("expected corruption detection, got %v", err)
	}

	// 7. DELETE
	if err := client.DeleteChunk(ctx, server.URL, id); err != nil {
		t.Fatal(err)
	}

	// 8. Repeated DELETE should also succeed.
	if err := client.DeleteChunk(ctx, server.URL, id); err != nil {
		t.Fatal(err)
	}

	// 9. GET must now return not found.
	_, err = client.GetChunk(
		ctx, server.URL, id, int64(len(data)), hash,
	)
	if !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}
