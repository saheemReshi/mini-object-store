package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"mini-object-store/internal/metadata"
	"mini-object-store/internal/storage"
	"mini-object-store/internal/worker"
)

func TestObjectAPI(t *testing.T) {
	registry := NewRegistry()
	dirs := make(map[string]string)

	// Start four storage workers.
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

		if _, _, err := registry.Register(id, server.URL); err != nil {
			t.Fatal(err)
		}
		if err := registry.Heartbeat(id); err != nil {
			t.Fatal(err)
		}
	}

	service := NewObjectService(
		registry, metadata.NewMemoryStore(), NewWorkerClient(),
	)

	api := httptest.NewServer(NewHandler(registry, service))
	defer api.Close()

	client := api.Client()

	// Upload a file larger than 1 MB.
	original := bytes.Repeat([]byte("A"), 1_200_000)

	var body bytes.Buffer
	form := multipart.NewWriter(&body)

	part, err := form.CreateFormFile("file", "test.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(original); err != nil {
		t.Fatal(err)
	}
	if err := form.WriteField("metadata", `{"owner":"alice"}`); err != nil {
		t.Fatal(err)
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}

	resp, err := client.Post(
		api.URL+"/api/v1/objects",
		form.FormDataContentType(),
		&body,
	)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload returned %d", resp.StatusCode)
	}

	var obj metadata.Object
	if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if obj.ChunkCount != 2 {
		t.Fatalf("expected 2 chunks, got %d", obj.ChunkCount)
	}

	objectURL := api.URL + "/api/v1/objects/" + obj.ID

	// Download and compare all bytes.
	checkDownload := func() {
		t.Helper()

		resp, err := client.Get(objectURL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("download returned %d", resp.StatusCode)
		}

		got, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, original) {
			t.Fatal("downloaded object differs from original")
		}
	}

	checkDownload()

	// Corrupt one replica directly on disk.
	chunk := obj.Chunks[0]
	corruptedPath := filepath.Join(
		dirs[chunk.Replicas[0]], chunk.ID,
	)

	if err := os.WriteFile(
		corruptedPath, []byte("corrupted"), 0600,
	); err != nil {
		t.Fatal(err)
	}

	// Download should recover using the second replica.
	checkDownload()

	// Patch custom metadata.
	patch := `{"custom_metadata":{"owner":"bob"}}`
	req, err := http.NewRequest(
		http.MethodPatch,
		objectURL+"/metadata",
		bytes.NewBufferString(patch),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var updated metadata.Object
	if err := json.NewDecoder(resp.Body).Decode(&updated); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK ||
		updated.CustomMetadata["owner"] != "bob" {
		t.Fatal("metadata patch failed")
	}

	// Delete the entire object.
	req, err = http.NewRequest(
		http.MethodDelete, objectURL, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete returned %d", resp.StatusCode)
	}

	// The object must no longer exist.
	resp, err = client.Get(objectURL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}
