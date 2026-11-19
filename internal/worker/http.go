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
	store    *storage.FileStore
	workerID string
}

// NewHandler exposes internal chunk and inventory routes. FileStore enforces
// size/hash validation before a PUT is acknowledged as successfully stored.
func NewHandler(
	store *storage.FileStore,
	workerID string,
) http.Handler {
	h := &Handler{
		store:    store,
		workerID: workerID,
	}

	mux := http.NewServeMux()

	mux.HandleFunc(
		"PUT /internal/chunks/{chunk_id}",
		h.putChunk,
	)

	mux.HandleFunc(
		"GET /internal/chunks/{chunk_id}",
		h.getChunk,
	)

	mux.HandleFunc(
		"DELETE /internal/chunks/{chunk_id}",
		h.deleteChunk,
	)

	mux.HandleFunc(
		"GET /internal/workers/{worker_id}/chunks",
		h.listChunks,
	)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern == "" {
			// Matched handlers retain the original writer for chunk streaming.
			w = &routeErrorWriter{ResponseWriter: w}
		}
		mux.ServeHTTP(w, r)
	})
}

// routeErrorWriter converts ServeMux's default errors without changing its
// status, Allow header, or redirects. It discards the original plain-text body.
type routeErrorWriter struct {
	http.ResponseWriter
	wroteError bool
}

func (w *routeErrorWriter) WriteHeader(status int) {
	if status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
		w.wroteError = true
		writeError(w.ResponseWriter, status, http.StatusText(status))
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *routeErrorWriter) Write(data []byte) (int, error) {
	if w.wroteError {
		return len(data), nil
	}
	return w.ResponseWriter.Write(data)
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

	// Carry cancellation into staging so a disconnected uploader can stop before
	// publication. The controller still cleans up uncertain completed writes.
	err = h.store.PutContext(
		r.Context(),
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

func (h *Handler) deleteChunk(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := r.PathValue("chunk_id")

	err := h.store.Delete(id)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrInvalidChunk):
			writeError(
				w,
				http.StatusBadRequest,
				"invalid chunk ID",
			)

		case errors.Is(err, os.ErrNotExist):
			writeError(
				w,
				http.StatusNotFound,
				"chunk not found",
			)

		default:
			log.Printf("DELETE chunk %s: %v", id, err)
			writeError(
				w,
				http.StatusInternalServerError,
				"storage failure",
			)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listChunks(
	w http.ResponseWriter,
	r *http.Request,
) {
	requestedID := r.PathValue("worker_id")

	// Ensure this request targets the correct worker.
	if requestedID != h.workerID {
		writeError(
			w,
			http.StatusNotFound,
			"worker not found",
		)
		return
	}

	chunks, err := h.store.List()
	if err != nil {
		log.Printf("list chunks: %v", err)

		writeError(
			w,
			http.StatusInternalServerError,
			"failed to list chunks",
		)
		return
	}

	response := struct {
		WorkerID string              `json:"worker_id"`
		Chunks   []storage.ChunkInfo `json:"chunks"`
	}{
		WorkerID: h.workerID,
		Chunks:   chunks,
	}

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("encode inventory: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
