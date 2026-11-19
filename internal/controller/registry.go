package controller

import (
	"errors"
	"net/url"
	"sort"
	"sync"
	"time"
)

var (
	ErrInvalidWorker  = errors.New("invalid worker registration")
	ErrWorkerConflict = errors.New("worker ID or address conflicts with an existing registration")
	ErrWorkerNotFound = errors.New("worker not registered")
)

// WorkerFailureTimeout is the maximum contact age before List reports unavailable.
const WorkerFailureTimeout = 6 * time.Second

type Worker struct {
	ID            string     `json:"worker_id"`
	Address       string     `json:"address"`
	RegisteredAt  time.Time  `json:"registered_at"`
	LastContact   time.Time  `json:"last_contact"`
	LastHeartbeat *time.Time `json:"last_heartbeat,omitempty"`
	Status        string     `json:"status"`
}

// Registry tracks in-memory membership. Health reflects recent heartbeats, not a
// disk-integrity check or a guarantee that the next worker RPC will succeed.
type Registry struct {
	mu      sync.RWMutex
	workers map[string]Worker
}

func NewRegistry() *Registry {
	return &Registry{
		workers: make(map[string]Worker),
	}
}

// Register returns the worker record, whether it was
// newly created, and any error.
func (r *Registry) Register(
	id, address string,
) (Worker, bool, error) {
	if !validWorkerID(id) || !validWorkerAddress(address) {
		return Worker{}, false, ErrInvalidWorker
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	for otherID, worker := range r.workers {
		if otherID != id && worker.Address == address {
			return Worker{}, false, ErrWorkerConflict
		}
	}

	if existing, ok := r.workers[id]; ok {
		if existing.Address != address {
			return Worker{}, false, ErrWorkerConflict
		}

		// An existing worker may have restarted.
		existing.LastContact = now
		existing.LastHeartbeat = nil
		existing.Status = "registered"

		r.workers[id] = existing
		return cloneWorker(existing), false, nil
	}

	worker := Worker{
		ID:           id,
		Address:      address,
		RegisteredAt: now,
		LastContact:  now,
		Status:       "registered",
	}

	r.workers[id] = worker
	return worker, true, nil

}

func cloneWorker(w Worker) Worker {
	if w.LastHeartbeat != nil {
		heartbeat := *w.LastHeartbeat
		w.LastHeartbeat = &heartbeat
	}
	return w
}

// List returns sorted, detached snapshots with health computed at read time.
// Failed workers remain registered so returning workers and stale replicas can
// still be addressed by their original IDs.
func (r *Registry) List() []Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()

	result := make([]Worker, 0, len(r.workers))

	for _, worker := range r.workers {
		// Derive health from the last contact time.
		if now.Sub(worker.LastContact) > WorkerFailureTimeout {
			worker.Status = "unavailable"
		} else if worker.LastHeartbeat == nil {
			worker.Status = "registered"
		} else {
			worker.Status = "healthy"
		}

		result = append(result, cloneWorker(worker))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result
}

func (r *Registry) Heartbeat(id string) error {
	if !validWorkerID(id) {
		return ErrInvalidWorker
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	worker, exists := r.workers[id]
	if !exists {
		return ErrWorkerNotFound
	}

	now := time.Now().UTC()

	worker.LastContact = now
	worker.LastHeartbeat = &now
	worker.Status = "healthy"

	r.workers[id] = worker
	return nil
}

func validWorkerID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}

	for _, c := range id {
		if !(c >= 'a' && c <= 'z' ||
			c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' ||
			c == '-' || c == '_') {
			return false
		}
	}

	return true
}

func validWorkerAddress(address string) bool {
	u, err := url.Parse(address)
	if err != nil {
		return false
	}

	return u.Scheme == "http" &&
		u.Hostname() != "" &&
		u.User == nil &&
		u.Path == "" &&
		u.RawQuery == "" &&
		u.Fragment == ""
}
