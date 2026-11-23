package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log"
	"os"

	"mini-object-store/internal/metadata"
)

var ErrChunkUnavailable = errors.New("no valid chunk replica available")
var ErrObjectCorrupt = errors.New("object integrity verification failed")

// Fetch a verified copy from one of the chunk's replicas.
func (s *ObjectService) fetchChunk(
	ctx context.Context,
	chunk metadata.Chunk,
) ([]byte, error) {
	rawHash, err := hex.DecodeString(chunk.Checksum)
	if err != nil || len(rawHash) != sha256.Size {
		return nil, ErrObjectCorrupt
	}

	var expectedHash [32]byte
	copy(expectedHash[:], rawHash)

	workers := make(map[string]Worker)
	for _, w := range s.registry.List() {
		workers[w.ID] = w
	}

	// Prefer workers whose heartbeats are current.
	// Still try unavailable workers afterward: status may be stale.
	for _, preferHealthy := range []bool{true, false} {
		for _, workerID := range chunk.Replicas {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			worker, exists := workers[workerID]
			if !exists {
				continue
			}

			healthy := worker.Status == "healthy"
			if healthy != preferHealthy {
				continue
			}

			data, err := s.client.GetChunk(
				ctx,
				worker.Address,
				chunk.ID,
				chunk.Size,
				expectedHash,
			)
			if err == nil {
				return data, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			log.Printf(
				"chunk %s: replica %s failed: %v",
				chunk.ID, workerID, err,
			)
		}
	}

	return nil, fmt.Errorf(
		"%w: %s", ErrChunkUnavailable, chunk.ID,
	)
}

// Download reconstructs the object into a verified temporary file.
// The caller owns the returned file and must close and remove it.
// The object read lock covers reconstruction; the independent temporary snapshot
// remains readable if DELETE runs after this method returns.
func (s *ObjectService) Download(
	ctx context.Context,
	id string,
) (*os.File, metadata.Object, error) {
	s.maintenance.RLock()
	defer s.maintenance.RUnlock()
	unlock := s.locks.lock(id, false)
	defer unlock()

	obj, err := s.store.GetObject(ctx, id)
	if err != nil {
		return nil, metadata.Object{}, err
	}
	if obj.Deleting {
		return nil, metadata.Object{}, metadata.ErrObjectDeleting
	}

	tmp, err := os.CreateTemp("", "object-download-*")
	if err != nil {
		return nil, metadata.Object{}, err
	}

	// On any error, remove the incomplete download.
	success := false
	defer func() {
		if !success {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()

	var objectHash hash.Hash = sha256.New()
	var totalSize int64

	if len(obj.Chunks) != obj.ChunkCount {
		return nil, metadata.Object{}, ErrObjectCorrupt
	}

	for i, chunk := range obj.Chunks {
		if err := ctx.Err(); err != nil {
			return nil, metadata.Object{}, err
		}

		if chunk.Index != i {
			return nil, metadata.Object{}, ErrObjectCorrupt
		}

		data, err := s.fetchChunk(ctx, chunk)
		if err != nil {
			return nil, metadata.Object{}, err
		}

		if _, err := tmp.Write(data); err != nil {
			return nil, metadata.Object{}, fmt.Errorf(
				"write download temp file: %w", err,
			)
		}

		_, _ = objectHash.Write(data)
		totalSize += int64(len(data))
	}

	// Per-chunk checks cannot detect every metadata error, such as duplicated
	// chunks. Verify the reconstructed size and whole-object digest as well.
	if totalSize != obj.Size ||
		hex.EncodeToString(objectHash.Sum(nil)) != obj.Checksum {
		return nil, metadata.Object{}, ErrObjectCorrupt
	}

	if _, err := tmp.Seek(0, 0); err != nil {
		return nil, metadata.Object{}, err
	}

	success = true
	return tmp, obj, nil
}
