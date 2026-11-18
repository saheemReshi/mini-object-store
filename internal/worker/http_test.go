package worker

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"mini-object-store/internal/storage"
)

func TestPutAndGetChunk(t *testing.T) {
	store, err := storage.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	handler := NewHandler(store, "worker1")
	data := []byte("hello from HTTP")

	// PUT
	putReq := httptest.NewRequest(
		http.MethodPut,
		"/internal/chunks/test-0",
		bytes.NewReader(data),
	)

	hash := sha256.Sum256(data)
	putReq.Header.Set("X-Chunk-SHA256", fmt.Sprintf("%x", hash))

	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, putReq)

	if putResp.Code != http.StatusCreated {
		t.Fatalf("PUT status: %d, body: %s",
			putResp.Code, putResp.Body.String())
	}

	// GET
	getReq := httptest.NewRequest(
		http.MethodGet,
		"/internal/chunks/test-0",
		nil,
	)

	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, getReq)

	if getResp.Code != http.StatusOK {
		t.Fatalf("GET status: %d", getResp.Code)
	}

	if !bytes.Equal(getResp.Body.Bytes(), data) {
		t.Fatal("downloaded data does not match")
	}
}

func TestDeleteAndInventory(t *testing.T) {
	store, err := storage.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	handler := NewHandler(store, "worker1")
	data := []byte("chunk for deletion test")
	hash := sha256.Sum256(data)

	// 1. Upload a chunk.
	putReq := httptest.NewRequest(
		http.MethodPut,
		"/internal/chunks/test-1",
		bytes.NewReader(data),
	)
	putReq.Header.Set("X-Chunk-SHA256", fmt.Sprintf("%x", hash))

	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, putReq)

	if putResp.Code != http.StatusCreated {
		t.Fatalf("PUT returned %d", putResp.Code)
	}

	// 2. Check the worker inventory.
	listReq := httptest.NewRequest(
		http.MethodGet,
		"/internal/workers/worker1/chunks",
		nil,
	)

	listResp := httptest.NewRecorder()
	handler.ServeHTTP(listResp, listReq)

	if listResp.Code != http.StatusOK {
		t.Fatalf("inventory returned %d", listResp.Code)
	}

	var inventory struct {
		WorkerID string              `json:"worker_id"`
		Chunks   []storage.ChunkInfo `json:"chunks"`
	}

	if err := json.Unmarshal(
		listResp.Body.Bytes(), &inventory,
	); err != nil {
		t.Fatal(err)
	}

	if inventory.WorkerID != "worker1" ||
		len(inventory.Chunks) != 1 ||
		inventory.Chunks[0].ID != "test-1" {
		t.Fatalf("unexpected inventory: %+v", inventory)
	}

	// 3. Delete the chunk.
	deleteReq := httptest.NewRequest(
		http.MethodDelete,
		"/internal/chunks/test-1",
		nil,
	)

	deleteResp := httptest.NewRecorder()
	handler.ServeHTTP(deleteResp, deleteReq)

	if deleteResp.Code != http.StatusNoContent {
		t.Fatalf("DELETE returned %d", deleteResp.Code)
	}

	// 4. Verify the chunk is gone.
	getReq := httptest.NewRequest(
		http.MethodGet,
		"/internal/chunks/test-1",
		nil,
	)

	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, getReq)

	if getResp.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE returned %d", getResp.Code)
	}

	// 5. A repeated DELETE returns 404.
	retryResp := httptest.NewRecorder()
	handler.ServeHTTP(retryResp, deleteReq)

	if retryResp.Code != http.StatusNotFound {
		t.Fatalf("repeated DELETE returned %d", retryResp.Code)
	}
}
