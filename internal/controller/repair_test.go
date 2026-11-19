package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"mini-object-store/internal/metadata"
	"mini-object-store/internal/storage"
	"mini-object-store/internal/worker"
)

func TestReplicaRepair(t *testing.T) {
	registry := NewRegistry()
	addresses := make(map[string]string)
	dirs := make(map[string]string)

	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("worker%d", i)
		dir := t.TempDir()
		dirs[id] = dir

		store, err := storage.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}

		server := httptest.NewServer(
			worker.NewHandler(store, id),
		)
		t.Cleanup(server.Close)

		addresses[id] = server.URL

		if _, _, err := registry.Register(id, server.URL); err != nil {
			t.Fatal(err)
		}
		if err := registry.Heartbeat(id); err != nil {
			t.Fatal(err)
		}
	}

	meta := metadata.NewMemoryStore()
	client := NewWorkerClient()
	service := NewObjectService(registry, meta, client)

	data := bytes.Repeat([]byte("A"), 1_200_000)

	obj, err := service.Upload(
		context.Background(),
		"test.bin",
		"application/octet-stream",
		nil,
		bytes.NewReader(data),
	)
	if err != nil {
		t.Fatal(err)
	}

	chunk := obj.Chunks[0]
	victim := chunk.Replicas[0]

	// Corrupt one physical copy.
	path := filepath.Join(dirs[victim], chunk.ID)
	if err := os.WriteFile(path, []byte("CORRUPT"), 0600); err != nil {
		t.Fatal(err)
	}

	// Run one repair cycle.
	if err := service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	updated, err := meta.GetObject(
		context.Background(), obj.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	repaired := updated.Chunks[0]

	if len(repaired.Replicas) != 2 {
		t.Fatal("replication factor not restored")
	}

	if repaired.Replicas[0] == repaired.Replicas[1] {
		t.Fatal("both replicas are on the same worker")
	}

	for _, id := range repaired.Replicas {
		if id == victim {
			t.Fatal("corrupted replica still in metadata")
		}
	}

	// Verify both new recorded replicas.
	decoded, _ := hex.DecodeString(repaired.Checksum)
	var hash [32]byte
	copy(hash[:], decoded)

	for _, workerID := range repaired.Replicas {
		got, err := client.GetChunk(
			context.Background(),
			addresses[workerID],
			repaired.ID,
			repaired.Size,
			hash,
		)
		if err != nil {
			t.Fatalf("replica %s: %v", workerID, err)
		}

		if !bytes.Equal(got, data[:int(repaired.Size)]) {
			t.Fatal("repaired data mismatch")
		}
	}
}
