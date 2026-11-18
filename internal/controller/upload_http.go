package controller

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

func (h *Handler) uploadObject(
	w http.ResponseWriter,
	r *http.Request,
) {
	// Limit the total multipart request to 1 GiB.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<30)

	// Large file parts are spooled to temporary disk files.
	if err := r.ParseMultipartForm(8 << 20); err != nil {
		var tooLarge *http.MaxBytesError

		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge,
				"upload too large")
		} else {
			writeError(w, http.StatusBadRequest,
				"invalid multipart upload")
		}
		return
	}
	defer r.MultipartForm.RemoveAll()

	files := r.MultipartForm.File["file"]
	if len(files) != 1 {
		writeError(w, http.StatusBadRequest,
			"expected exactly one file field")
		return
	}

	fields := r.MultipartForm.Value["metadata"]
	if len(fields) > 1 {
		writeError(w, http.StatusBadRequest,
			"expected at most one metadata field")
		return
	}

	custom := map[string]string{}

	if len(fields) == 1 {
		err := json.Unmarshal([]byte(fields[0]), &custom)
		if err != nil || custom == nil {
			writeError(w, http.StatusBadRequest,
				"metadata must be a JSON object with string values")
			return
		}
	}

	file, err := files[0].Open()
	if err != nil {
		writeError(w, http.StatusInternalServerError,
			"cannot open uploaded file")
		return
	}
	defer file.Close()

	object, err := h.objects.Upload(
		r.Context(),
		files[0].Filename,
		files[0].Header.Get("Content-Type"),
		custom,
		file,
	)

	if err != nil {
		switch {
		case errors.Is(err, ErrInsufficientWorkers),
			errors.Is(err, ErrReplicationFailed):
			writeError(w, http.StatusServiceUnavailable,
				err.Error())

		default:
			log.Printf("upload: %v", err)
			writeError(w, http.StatusInternalServerError,
				"upload failed")
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location",
		"/api/v1/objects/"+object.ID)
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(object)
}
