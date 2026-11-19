package controller

import (
	"net/http"
)

type Handler struct {
	registry *Registry
	objects  *ObjectService
}

func NewHandler(
	registry *Registry,
	objects *ObjectService,
) http.Handler {
	h := &Handler{
		registry: registry,
		objects:  objects,
	}

	mux := http.NewServeMux()

	mux.HandleFunc(
		"POST /internal/workers/register",
		h.registerWorker,
	)

	// Diagnostic endpoint to inspect the registry.
	mux.HandleFunc(
		"GET /internal/workers",
		h.listWorkers,
	)
	mux.HandleFunc(
		"POST /internal/workers/heartbeat",
		h.heartbeatWorker,
	)

	if objects != nil {
		mux.HandleFunc(
			"POST /api/v1/objects",
			h.uploadObject,
		)
		mux.HandleFunc(
			"GET /api/v1/objects/{object_id}",
			h.downloadObject,
		)
		mux.HandleFunc(
			"GET /api/v1/objects/{object_id}/metadata",
			h.getObjectMetadata,
		)
		mux.HandleFunc(
			"PATCH /api/v1/objects/{object_id}/metadata",
			h.patchObjectMetadata,
		)
		mux.HandleFunc(
			"DELETE /api/v1/objects/{object_id}",
			h.deleteObject,
		)
		mux.HandleFunc(
			"GET /api/v1/objects",
			h.listObjects,
		)
	}

	return mux
}
