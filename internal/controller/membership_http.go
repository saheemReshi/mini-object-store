package controller

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

func (h *Handler) heartbeatWorker(
	w http.ResponseWriter,
	r *http.Request,
) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	defer r.Body.Close()

	var req struct {
		WorkerID string `json:"worker_id"`
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid heartbeat JSON")
		return
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "expected one JSON object")
		return
	}

	err := h.registry.Heartbeat(req.WorkerID)

	switch {
	case errors.Is(err, ErrInvalidWorker):
		writeError(w, http.StatusBadRequest, err.Error())

	case errors.Is(err, ErrWorkerNotFound):
		writeError(w, http.StatusNotFound, "worker not registered")

	case err != nil:
		writeError(w, http.StatusInternalServerError, "heartbeat failed")

	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *Handler) registerWorker(
	w http.ResponseWriter,
	r *http.Request,
) {
	// A registration request should be tiny.
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	defer r.Body.Close()

	var req struct {
		WorkerID string `json:"worker_id"`
		Address  string `json:"address"`
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid registration JSON")
		return
	}

	// Reject additional JSON values after the first object.
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "expected exactly one JSON object")
		return
	}

	worker, created, err := h.registry.Register(
		req.WorkerID,
		req.Address,
	)

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidWorker):
			writeError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, ErrWorkerConflict):
			writeError(w, http.StatusConflict, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "registration failed")
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if created {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	_ = json.NewEncoder(w).Encode(worker)
}

func (h *Handler) listWorkers(
	w http.ResponseWriter,
	r *http.Request,
) {
	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(struct {
		Workers []Worker `json:"workers"`
	}{
		Workers: h.registry.List(),
	})
}
