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

func TestRouterErrorsAreJSON(t *testing.T) {
	store, err := storage.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(store, "worker1")
	for _, tc := range []struct {
		method, path string
		status       int
		allow        string
	}{
		{http.MethodGet, "/unknown", http.StatusNotFound, ""},
		{http.MethodPost, "/unknown", http.StatusNotFound, ""},
		{http.MethodPost, "/internal/chunks/test-0", http.StatusMethodNotAllowed, "DELETE, GET, HEAD, PUT"},
		{http.MethodOptions, "/internal/workers/worker1/chunks", http.StatusMethodNotAllowed, "GET, HEAD"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.status || response.Header().Get("Allow") != tc.allow {
				t.Fatalf("status=%d Allow=%q; want %d %q", response.Code, response.Header().Get("Allow"), tc.status, tc.allow)
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("Content-Type=%q, body=%q", got, response.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("expected one JSON error object, got %q (%v)", response.Body.String(), err)
			}
		})
	}
}

func TestRouterPreservesHeadAndRedirects(t *testing.T) {
	store, err := storage.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(store, "worker1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/internal/workers/worker1/chunks", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("HEAD status=%d", response.Code)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal//workers/worker1/chunks", nil))
	if response.Code != http.StatusTemporaryRedirect || response.Header().Get("Location") != "/internal/workers/worker1/chunks" {
		t.Fatalf("redirect status=%d Location=%q", response.Code, response.Header().Get("Location"))
	}
}
