package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mini-object-store/internal/metadata"
)

func TestRouterErrorsAreJSON(t *testing.T) {
	registry := NewRegistry()
	handler := NewHandler(registry, NewObjectService(registry, metadata.NewMemoryStore(), NewWorkerClient()))

	for _, tc := range []struct {
		method, path string
		status       int
		allow        string
	}{
		{http.MethodGet, "/unknown", http.StatusNotFound, ""},
		{http.MethodPost, "/unknown", http.StatusNotFound, ""},
		{http.MethodOptions, "/api/v1/objects", http.StatusMethodNotAllowed, "GET, HEAD, POST"},
		{http.MethodPut, "/api/v1/objects/object-1", http.StatusMethodNotAllowed, "DELETE, GET, HEAD"},
		{http.MethodPost, "/api/v1/objects/object-1/metadata", http.StatusMethodNotAllowed, "GET, HEAD, PATCH"},
		{http.MethodGet, "/internal/workers/register", http.StatusMethodNotAllowed, "POST"},
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
	handler := NewHandler(NewRegistry(), nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/internal/workers", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("HEAD status=%d", response.Code)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal//workers", nil))
	if response.Code != http.StatusTemporaryRedirect || response.Header().Get("Location") != "/internal/workers" {
		t.Fatalf("redirect status=%d Location=%q", response.Code, response.Header().Get("Location"))
	}
}
