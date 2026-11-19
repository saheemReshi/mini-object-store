package controller

import (
	"encoding/json"
	"net/http"
)

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

func writeError(
	w http.ResponseWriter,
	status int,
	message string,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
