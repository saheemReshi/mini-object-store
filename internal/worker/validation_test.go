package worker

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"

	"mini-object-store/internal/storage"
)

func TestRejectInvalidHTTPChunkWrites(t *testing.T) {
	dir := t.TempDir()
	fs, err := storage.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(fs, "worker")
	for _, tc := range []struct {
		name, hash, body string
		length           int64
	}{
		{"empty", "", "", 0},
		{"unknown-size", "", "x", -1},
		{"oversize", "", "x", storage.ChunkSize + 1},
		{"missing-hash", "", "x", 1},
		{"invalid-hash", "zz", "x", 1},
		{"checksum-mismatch", fmt.Sprintf("%x", sha256.Sum256([]byte("y"))), "x", 1},
		{"short", fmt.Sprintf("%x", sha256.Sum256([]byte("x"))), "x", 2},
		{"long", fmt.Sprintf("%x", sha256.Sum256([]byte("xy"))), "xy", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("PUT", "/internal/chunks/chunk", bytes.NewBufferString(tc.body))
			r.ContentLength = tc.length
			r.Header.Set("X-Chunk-SHA256", tc.hash)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 400 {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("invalid HTTP write left files: %v %v", entries, err)
			}
		})
	}
}
