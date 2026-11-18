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
	ErrWorkerConflict = errors.New("worker ID already registered with a different address")
)

type Worker struct {
	ID           string    `json:"worker_id"`
	Address      string    `json:"address"`
	RegisteredAt time.Time `json:"registered_at"`
}

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

	if existing, ok := r.workers[id]; ok {
		if existing.Address != address {
			return Worker{}, false, ErrWorkerConflict
		}
		return existing, false, nil
	}

	worker := Worker{
		ID:           id,
		Address:      address,
		RegisteredAt: time.Now().UTC(),
	}

	r.workers[id] = worker
	return worker, true, nil
}

func (r *Registry) List() []Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]Worker, 0, len(r.workers))

	for _, worker := range r.workers {
		result = append(result, worker)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result
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
