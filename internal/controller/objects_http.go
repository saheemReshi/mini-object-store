package controller

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"mini-object-store/internal/metadata"
)

func (h *Handler) downloadObject(
	w http.ResponseWriter,
	r *http.Request,
) {
	id := r.PathValue("object_id")

	file, obj, err := h.objects.Download(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, metadata.ErrObjectNotFound):
			writeError(w, http.StatusNotFound, "object not found")

		case errors.Is(err, ErrChunkUnavailable),
			errors.Is(err, ErrObjectCorrupt):
			log.Printf("download %s: %v", id, err)
			writeError(w, http.StatusServiceUnavailable,
				"object data unavailable or corrupt")

		case errors.Is(err, metadata.ErrObjectDeleting):
			writeError(w, http.StatusServiceUnavailable,
				"object deletion in progress")

		default:
			log.Printf("download %s: %v", id, err)
			writeError(w, http.StatusInternalServerError,
				"download failed")
		}
		return
	}
	defer removeTempFile(file.Name())
	defer file.Close()

	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length",
		strconv.FormatInt(obj.Size, 10))
	w.Header().Set("X-Content-Type-Options", "nosniff")

	w.WriteHeader(http.StatusOK)

	if _, err := io.Copy(w, file); err != nil {
		log.Printf("stream object %s: %v", id, err)
	}
}

func removeTempFile(path string) {
	if err := os.Remove(path); err != nil {
		log.Printf("remove temporary download %s: %v", path, err)
	}
}

func (h *Handler) getObjectMetadata(
	w http.ResponseWriter,
	r *http.Request,
) {
	obj, err := h.objects.GetMetadata(
		r.Context(), r.PathValue("object_id"),
	)
	if err != nil {
		handleMetadataError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, obj)
}

func (h *Handler) patchObjectMetadata(
	w http.ResponseWriter,
	r *http.Request,
) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	defer r.Body.Close()

	var req struct {
		CustomMetadata map[string]string `json:"custom_metadata"`
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid metadata JSON")
		return
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF ||
		req.CustomMetadata == nil {
		writeError(w, http.StatusBadRequest,
			"expected custom_metadata object")
		return
	}

	obj, err := h.objects.PatchMetadata(
		r.Context(),
		r.PathValue("object_id"),
		req.CustomMetadata,
	)
	if err != nil {
		handleMetadataError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, obj)
}

func (h *Handler) listObjects(
	w http.ResponseWriter,
	r *http.Request,
) {
	objects, err := h.objects.ListObjects(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError,
			"failed to list objects")
		return
	}

	type summary struct {
		ID          string `json:"object_id"`
		Name        string `json:"name"`
		Size        int64  `json:"size"`
		ContentType string `json:"content_type"`
	}

	result := make([]summary, 0, len(objects))

	for _, obj := range objects {
		result = append(result, summary{
			ID:          obj.ID,
			Name:        obj.Name,
			Size:        obj.Size,
			ContentType: obj.ContentType,
		})
	}

	writeJSON(w, http.StatusOK, struct {
		Objects []summary `json:"objects"`
	}{
		Objects: result,
	})
}

func (h *Handler) deleteObject(
	w http.ResponseWriter,
	r *http.Request,
) {
	err := h.objects.DeleteObject(
		r.Context(), r.PathValue("object_id"),
	)
	if err != nil {
		switch {
		case errors.Is(err, metadata.ErrObjectNotFound):
			writeError(w, http.StatusNotFound, "object not found")

		case errors.Is(err, ErrDeletionIncomplete):
			log.Printf("delete object: %v", err)
			writeError(w, http.StatusServiceUnavailable,
				"some chunk replicas could not be deleted; retry")

		default:
			log.Printf("delete object: %v", err)
			writeError(w, http.StatusInternalServerError,
				"object deletion failed")
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleMetadataError(w http.ResponseWriter, err error) {
	if errors.Is(err, metadata.ErrObjectNotFound) {
		writeError(w, http.StatusNotFound, "object not found")
	} else if errors.Is(err, metadata.ErrObjectDeleting) {
		writeError(w, http.StatusServiceUnavailable,
			"object deletion in progress")
	} else {
		log.Printf("metadata: %v", err)
		writeError(w, http.StatusInternalServerError,
			"metadata operation failed")
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
