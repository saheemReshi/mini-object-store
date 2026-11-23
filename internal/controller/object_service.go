package controller

import (
	"sync"

	"mini-object-store/internal/metadata"
)

// ObjectService coordinates object metadata and worker replicas. Callers should
// share one service instance so its object locks protect every operation.
type ObjectService struct {
	maintenance    sync.RWMutex // GC excludes object operations, including repair.
	registry       *Registry
	store          metadata.Store
	client         *WorkerClient
	locks          *objectLockTable
	cleanupMu      sync.Mutex // Protects the pending set; never held during RPCs.
	cleanupRunMu   sync.Mutex // Prevents overlapping cleanup passes.
	pendingCleanup map[replicaTarget]struct{}
}

// NewObjectService wires existing dependencies; it does not start repair.
func NewObjectService(
	registry *Registry,
	store metadata.Store,
	client *WorkerClient,
) *ObjectService {
	return &ObjectService{
		registry:       registry,
		store:          store,
		client:         client,
		locks:          newObjectLockTable(),
		pendingCleanup: make(map[replicaTarget]struct{}),
	}
}
