package worker

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"

	"mini-object-store/internal/storage"
)

type Handler struct {
	store *storage.FileStore
}

func NewHandler(store *storage.FileStore) http.Handler {
	h := &Handler{store: store}

	mux := http.NewServeMux()

	mux.HandleFunc(
		"PUT /internal/chunks/{chunk_id}",
		h.putChunk,
	)

	mux.HandleFunc(
		"GET /internal/chunks/{chunk_id}",
		h.getChunk,
	)

	return mux
}

func (h *Handler) putChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")

	if r.ContentLength <= 0 ||
		r.ContentLength > storage.ChunkSize {
		writeError(w, http.StatusBadRequest, "invalid chunk size")
		return
	}

	// Decode the expected SHA-256 hash.
	rawHash := r.Header.Get("X-Chunk-SHA256")

	decoded, err := hex.DecodeString(rawHash)
	if err != nil || len(decoded) != 32 {
		writeError(w, http.StatusBadRequest, "invalid SHA-256 header")
		return
	}

	var expectedHash [32]byte
	copy(expectedHash[:], decoded)

	// HTTP request body is an io.Reader.
	err = h.store.Put(
		id,
		r.Body,
		r.ContentLength,
		expectedHash,
	)

	if err != nil {
		switch {
		case errors.Is(err, storage.ErrInvalidChunk):
			writeError(w, http.StatusBadRequest, err.Error())

		case errors.Is(err, storage.ErrChunkConflict):
			writeError(w, http.StatusConflict, err.Error())

		default:
			log.Printf("PUT chunk %s: %v", id, err)
			writeError(w, http.StatusInternalServerError, "storage failure")
		}
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) getChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")

	reader, err := h.store.Get(id)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			writeError(w, http.StatusNotFound, "chunk not found")

		case errors.Is(err, storage.ErrInvalidChunk):
			writeError(w, http.StatusBadRequest, "invalid chunk ID")

		default:
			log.Printf("GET chunk %s: %v", id, err)
			writeError(w, http.StatusInternalServerError, "storage failure")
		}
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")

	if _, err := io.Copy(w, reader); err != nil {
		log.Printf("stream chunk %s: %v", id, err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
