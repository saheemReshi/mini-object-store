package controller

import (
	"context"
	"log"
	"time"
)

type replicaTarget struct {
	address string
	chunkID string
}

// Failed cleanup is retained until a later repair pass can reach the worker.
// The queue is in memory only; explicit startup GC can reclaim unreferenced
// published chunks if a controller crash loses these cleanup attempts.
func (s *ObjectService) cleanup(targets []replicaTarget) {
	s.cleanupMu.Lock()
	for _, target := range targets {
		s.pendingCleanup[target] = struct{}{}
	}
	s.cleanupMu.Unlock()
	// The original upload context may already be canceled. Give rollback its
	// own bounded lifetime so disconnecting a client does not skip cleanup.
	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()
	s.retryCleanup(ctx, targets)
}

func (s *ObjectService) retryCleanup(ctx context.Context, targets []replicaTarget) {
	// Serialize retries so an old successful DELETE cannot acknowledge a newer
	// failed attempt for the same target. Chunk IDs are unique across uploads.
	s.cleanupRunMu.Lock()
	defer s.cleanupRunMu.Unlock()
	for _, target := range targets {
		if ctx.Err() != nil {
			return
		}
		s.cleanupMu.Lock()
		_, pending := s.pendingCleanup[target]
		s.cleanupMu.Unlock()
		if !pending {
			continue
		}
		err := s.client.DeleteChunk(
			ctx, target.address, target.chunkID,
		)
		if err != nil {
			log.Printf(
				"cleanup %s on %s: %v",
				target.chunkID, target.address, err,
			)
		} else {
			s.cleanupMu.Lock()
			delete(s.pendingCleanup, target)
			s.cleanupMu.Unlock()
		}
	}
}

func (s *ObjectService) repairCleanup(ctx context.Context) {
	// Limit orphan work per scan so an unreachable worker does not consume all
	// of the time available for checking replicas of successfully stored objects.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	s.cleanupMu.Lock()
	targets := make([]replicaTarget, 0, len(s.pendingCleanup))
	for target := range s.pendingCleanup {
		targets = append(targets, target)
	}
	s.cleanupMu.Unlock()
	s.retryCleanup(ctx, targets)
}
