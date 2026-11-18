package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"mini-object-store/internal/metadata"
	"mini-object-store/internal/storage"
	"mini-object-store/internal/worker"
)

func TestDistributedUpload(t *testing.T) {
	registry := NewRegistry()
	addresses := make(map[string]string)

	// Start four independent HTTP workers.
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("worker%d", i)

		store, err := storage.NewFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}

		server := httptest.NewServer(
			worker.NewHandler(store, id),
		)
		t.Cleanup(server.Close)

		if _, _, err := registry.Register(
			id, server.URL,
		); err != nil {
			t.Fatal(err)
		}

		if err := registry.Heartbeat(id); err != nil {
			t.Fatal(err)
		}

		addresses[id] = server.URL
	}

	meta := metadata.NewMemoryStore()
	client := NewWorkerClient()
	service := NewObjectService(registry, meta, client)

	// Slightly over 2 MB: must create three chunks.
	data := bytes.Repeat(
		[]byte{0x42},
		int(storage.ChunkSize)*2+17,
	)

	obj, err := service.Upload(
		context.Background(),
		"test.bin",
		"application/octet-stream",
		map[string]string{"owner": "test"},
		bytes.NewReader(data),
	)
	if err != nil {
		t.Fatal(err)
	}

	if obj.ChunkCount != 3 ||
		obj.Size != int64(len(data)) {
		t.Fatalf("unexpected object: %+v", obj)
	}

	if obj.Checksum != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Fatal("incorrect whole-object checksum")
	}

	// Verify both physical replicas of every chunk.
	for _, chunk := range obj.Chunks {
		if len(chunk.Replicas) != 2 ||
			chunk.Replicas[0] == chunk.Replicas[1] {
			t.Fatalf("invalid replication: %+v", chunk)
		}

		decoded, err := hex.DecodeString(chunk.Checksum)
		if err != nil {
			t.Fatal(err)
		}

		var digest [32]byte
		copy(digest[:], decoded)

		start := int64(chunk.Index) * storage.ChunkSize
		expected := data[int(start):int(start+chunk.Size)]

		for _, workerID := range chunk.Replicas {
			got, err := client.GetChunk(
				context.Background(),
				addresses[workerID],
				chunk.ID,
				chunk.Size,
				digest,
			)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got, expected) {
				t.Fatalf("wrong data on %s", workerID)
			}
		}
	}

	// Confirm metadata was published.
	if _, err := meta.GetObject(
		context.Background(), obj.ID,
	); err != nil {
		t.Fatal(err)
	}

	// No healthy workers => upload must fail.
	emptyService := NewObjectService(
		NewRegistry(), meta, client,
	)

	_, err = emptyService.Upload(
		context.Background(),
		"x.bin", "", nil,
		bytes.NewReader(data),
	)

	if !errors.Is(err, ErrInsufficientWorkers) {
		t.Fatalf("expected insufficient workers: %v", err)
	}
}
