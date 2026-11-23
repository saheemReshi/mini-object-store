package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"time"

	"mini-object-store/internal/metadata"
)

// RunRepair checks replicas periodically until ctx is canceled. Pass a positive
// interval; each scan finishes before the next one starts in this goroutine.
func (s *ObjectService) RunRepair(
	ctx context.Context,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := s.RepairOnce(ctx); err != nil {
				log.Printf("repair scan: %v", err)
			}
		}
	}
}

// RepairOnce retries orphan cleanup and scans live objects. Per-chunk failures
// are logged; a nil result does not guarantee that every object is recoverable.
func (s *ObjectService) RepairOnce(
	ctx context.Context,
) error {
	s.maintenance.RLock()
	defer s.maintenance.RUnlock()
	s.repairCleanup(ctx)
	objects, err := s.store.ListObjects(ctx)
	if err != nil {
		return err
	}

	for _, entry := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Hold the object lock across worker RPCs and metadata publication so a
		// concurrent DELETE cannot remove data that this repair would resurrect.
		unlock := s.locks.lock(entry.ID, true)

		obj, err := s.store.GetObject(ctx, entry.ID)

		if err == nil && !obj.Deleting {
			for _, chunk := range obj.Chunks {
				if err := ctx.Err(); err != nil {
					unlock()
					return err
				}

				s.repairChunk(ctx, obj.ID, chunk)
			}
		}

		unlock()

		// An object may disappear between List and Get.
		if err != nil &&
			!errors.Is(err, metadata.ErrObjectNotFound) {
			log.Printf(
				"repair object %s: %v", entry.ID, err,
			)
		}
	}

	return nil
}

func (s *ObjectService) repairChunk(
	ctx context.Context,
	objectID string,
	chunk metadata.Chunk,
) {
	raw, err := hex.DecodeString(chunk.Checksum)
	if err != nil || len(raw) != sha256.Size {
		log.Printf("invalid checksum metadata: %s", chunk.ID)
		return
	}

	var checksum [32]byte
	copy(checksum[:], raw)

	workers := s.registry.List()
	byID := make(map[string]Worker, len(workers))

	for _, w := range workers {
		byID[w.ID] = w
	}

	var data []byte
	valid := make([]string, 0, 2)

	// Check both recorded replicas.
	for _, workerID := range chunk.Replicas {
		w, exists := byID[workerID]

		if !exists || w.Status != "healthy" || containsWorker(valid, workerID) {
			continue
		}

		b, err := s.client.GetChunk(
			ctx,
			w.Address,
			chunk.ID,
			chunk.Size,
			checksum,
		)

		if err != nil {
			log.Printf(
				"replica %s/%s invalid: %v",
				workerID, chunk.ID, err,
			)
			continue
		}

		// Retain one verified source for repair.
		if len(valid) == 0 {
			data = b
		}
		valid = append(valid, workerID)
	}

	// Both replicas verified successfully.
	if len(valid) >= 2 {
		s.removeExtraReplicas(ctx, chunk, valid, workers)
		return
	}
	if len(valid) == 0 {
		// A previous repair may have written data but failed to publish metadata.
		// Search those untracked locations before declaring the chunk lost.
		for _, w := range workers {
			if w.Status != "healthy" || containsWorker(chunk.Replicas, w.ID) {
				continue
			}
			b, err := s.client.GetChunk(ctx, w.Address, chunk.ID, chunk.Size, checksum)
			if err == nil {
				valid = append(valid, w.ID)
				data = b
				break
			}
		}
	}

	// Prefer spare workers, then allow restoring a bad recorded replica when
	// there is no spare. Probe existing copies first: a returning worker can
	// hold a verified copy no longer recorded in metadata.
	candidates := make([]Worker, 0, len(workers))
	for _, recorded := range []bool{false, true} {
		for _, w := range workers {
			if w.Status == "healthy" && !containsWorker(valid, w.ID) &&
				containsWorker(chunk.Replicas, w.ID) == recorded {
				candidates = append(candidates, w)
			}
		}
	}
	newTargets := make([]replicaTarget, 0)
	for _, w := range candidates {
		if ctx.Err() != nil {
			break
		}
		b, getErr := s.client.GetChunk(ctx, w.Address, chunk.ID, chunk.Size, checksum)
		if getErr != nil {
			if len(valid) == 0 || (!errors.Is(getErr, ErrChunkCorrupt) && !errors.Is(getErr, ErrChunkNotFound)) {
				continue
			}
			if errors.Is(getErr, ErrChunkCorrupt) {
				if err := s.client.DeleteChunk(ctx, w.Address, chunk.ID); err != nil {
					continue
				}
			}
			// Track even ambiguous PUT failures; a timeout can follow publication.
			if !containsWorker(chunk.Replicas, w.ID) {
				newTargets = append(newTargets, replicaTarget{address: w.Address, chunkID: chunk.ID})
			}
			if err := s.client.PutChunk(ctx, w.Address, chunk.ID, data, checksum); err != nil {
				log.Printf("replacement %s on %s failed: %v", chunk.ID, w.ID, err)
				continue
			}
		} else if len(valid) == 0 {
			data = b
		}
		valid = append(valid, w.ID)
		if len(valid) < 2 {
			continue
		}

		// Compare-and-swap the old ordered placement. A future persistent Store
		// must check both placement and the tombstone in the same transaction.
		err := s.store.UpdateChunkReplicas(
			ctx,
			objectID,
			chunk.ID,
			chunk.Replicas,
			valid,
		)

		if err != nil {
			// Never remove a restored replica already referenced by metadata.
			s.rollbackRepair(newTargets)
			log.Printf("replica metadata update failed: %v", err)
			return
		}

		log.Printf(
			"repaired %s: %v -> %v",
			chunk.ID, chunk.Replicas, valid,
		)
		// Failed attempts on other destinations may also have published data.
		// Reconciliation retries their removal while this object exists.
		s.removeExtraReplicas(ctx, chunk, valid, workers)
		return
	}

	s.rollbackRepair(newTargets)
	log.Printf(
		"under-replicated %s: %d verified replicas", chunk.ID, len(valid),
	)
}

// Unlike failed-upload chunks, these belong to a live object. Do not enqueue
// delayed DELETEs: a future repair may adopt one of these untracked copies.
// Any failed rollback is reconciled by a later scan (or object deletion).
func (s *ObjectService) rollbackRepair(targets []replicaTarget) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, target := range targets {
		if err := s.client.DeleteChunk(ctx, target.address, target.chunkID); err != nil {
			log.Printf("rollback repair %s on %s: %v", target.chunkID, target.address, err)
		}
	}
}

// Called only after two distinct copies have been verified and their locations
// committed. The object lock prevents deletion/repair from racing this cleanup.
func (s *ObjectService) removeExtraReplicas(ctx context.Context, chunk metadata.Chunk, keep []string, workers []Worker) {
	for _, w := range workers {
		if ctx.Err() != nil {
			return
		}
		if w.Status != "healthy" || containsWorker(keep, w.ID) {
			continue
		}
		if err := s.client.DeleteChunk(ctx, w.Address, chunk.ID); err != nil {
			log.Printf("remove stale replica %s on %s: %v", chunk.ID, w.ID, err)
		}
	}
}

func containsWorker(ids []string, id string) bool {
	for _, existing := range ids {
		if existing == id {
			return true
		}
	}
	return false
}
