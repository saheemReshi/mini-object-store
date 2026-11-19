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

func (s *ObjectService) RepairOnce(
	ctx context.Context,
) error {
	objects, err := s.store.ListObjects(ctx)
	if err != nil {
		return err
	}

	for _, entry := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Exclusive object lock: prevent concurrent deletion.
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

	var source string
	var data []byte
	valid := 0

	// Check both recorded replicas.
	for _, workerID := range chunk.Replicas {
		w, exists := byID[workerID]

		if !exists || w.Status != "healthy" {
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

		valid++

		// Retain one verified source for repair.
		if source == "" {
			source = workerID
			data = b
		}
	}

	// Both replicas verified successfully.
	if valid >= 2 {
		return
	}

	// No source exists from which to reconstruct the chunk.
	if valid == 0 {
		log.Printf(
			"UNRECOVERABLE: no valid replica for %s",
			chunk.ID,
		)
		return
	}

	// Find a healthy worker not already in replica metadata.
	for _, w := range workers {
		if w.Status != "healthy" ||
			containsWorker(chunk.Replicas, w.ID) {
			continue
		}

		// Upload the verified bytes.
		err := s.client.PutChunk(
			ctx, w.Address, chunk.ID, data, checksum,
		)
		if err != nil {
			log.Printf(
				"replacement %s on %s failed: %v",
				chunk.ID, w.ID, err,
			)
			continue
		}

		replacement := []string{source, w.ID}

		// Publish the new placement atomically.
		err = s.store.UpdateChunkReplicas(
			ctx,
			objectID,
			chunk.ID,
			chunk.Replicas,
			replacement,
		)

		if err != nil {
			// Roll back the newly written replica if metadata
			// publication failed.
			cleanupCtx, cancel := context.WithTimeout(
				context.Background(), 5*time.Second,
			)

			if cleanupErr := s.client.DeleteChunk(
				cleanupCtx, w.Address, chunk.ID,
			); cleanupErr != nil {
				log.Printf(
					"cleanup replacement %s on %s: %v",
					chunk.ID, w.ID, cleanupErr,
				)
			}
			cancel()

			log.Printf("replica metadata update failed: %v", err)
			return
		}

		log.Printf(
			"repaired %s: %v -> %v",
			chunk.ID, chunk.Replicas, replacement,
		)
		return
	}

	log.Printf(
		"under-replicated %s: no suitable destination",
		chunk.ID,
	)
}

func containsWorker(ids []string, id string) bool {
	for _, existing := range ids {
		if existing == id {
			return true
		}
	}
	return false
}
