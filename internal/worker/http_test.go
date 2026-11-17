package worker

import (
	"bytes"
	"crypto/sha256"
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

	handler := NewHandler(store)
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
