package controller

import (
	"sync"

	"mini-object-store/internal/metadata"
)

type ObjectService struct {
	registry       *Registry
	store          metadata.Store
	client         *WorkerClient
	locks          *objectLockTable
	cleanupMu      sync.Mutex
	cleanupRunMu   sync.Mutex
	pendingCleanup map[replicaTarget]struct{}
}

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
