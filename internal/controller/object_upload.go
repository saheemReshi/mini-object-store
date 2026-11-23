package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"mini-object-store/internal/metadata"
	"mini-object-store/internal/storage"
)

var (
	ErrInsufficientWorkers = errors.New("fewer than two healthy workers")
	ErrReplicationFailed   = errors.New("chunk replication failed")
)

// Upload creates a new object and publishes metadata only after both replicas of
// every chunk acknowledge storage. A failed upload is queued for physical cleanup.
func (s *ObjectService) Upload(
	ctx context.Context,
	name, contentType string,
	custom map[string]string,
	src io.Reader,
) (metadata.Object, error) {
	s.maintenance.RLock()
	defer s.maintenance.RUnlock()

	// Obtain a snapshot of healthy workers.
	healthy := make([]Worker, 0)
	for _, w := range s.registry.List() {
		if w.Status == "healthy" {
			healthy = append(healthy, w)
		}
	}

	if len(healthy) < 2 {
		return metadata.Object{}, ErrInsufficientWorkers
	}

	// Generate an unpredictable unique object ID.
	var randomID [16]byte
	if _, err := rand.Read(randomID[:]); err != nil {
		return metadata.Object{}, err
	}

	id := "obj-" + hex.EncodeToString(randomID[:])
	start := int(randomID[0]) % len(healthy)

	if contentType == "" {
		contentType = "application/octet-stream"
	}

	objectHash := sha256.New()
	buffer := make([]byte, int(storage.ChunkSize))

	chunks := make([]metadata.Chunk, 0)
	targets := make([]replicaTarget, 0)
	var totalSize int64

	// Read the input file incrementally.
	for index := 0; ; index++ {
		if err := ctx.Err(); err != nil {
			s.cleanup(targets)
			return metadata.Object{}, err
		}
		// EOF means no next chunk; ErrUnexpectedEOF carries a valid final partial
		// chunk. This also avoids creating a zero-byte chunk for an empty object.
		n, readErr := io.ReadFull(src, buffer)

		if errors.Is(readErr, io.EOF) {
			break
		}

		if readErr != nil &&
			!errors.Is(readErr, io.ErrUnexpectedEOF) {
			s.cleanup(targets)
			return metadata.Object{}, fmt.Errorf(
				"read upload: %w", readErr,
			)
		}

		data := buffer[:n]
		_, _ = objectHash.Write(data)

		digest := sha256.Sum256(data)
		chunkID := fmt.Sprintf("%s-%d", id, index)

		// Rotate placement across workers; adjacent positions are distinct because
		// the healthy snapshot contains at least two registered worker IDs.
		first := (start + index) % len(healthy)

		replicas := [2]Worker{
			healthy[first],
			healthy[(first+1)%len(healthy)],
		}

		// Replicate concurrently.
		results := make(chan error, 2)

		for _, w := range replicas {
			// Record attempts before sending: a failed response does not prove the
			// worker failed to publish, so cleanup must include uncertain writes.
			targets = append(targets, replicaTarget{
				address: w.Address,
				chunkID: chunkID,
			})

			go func(w Worker) {
				results <- s.client.PutChunk(
					ctx, w.Address, chunkID, data, digest,
				)
			}(w)
		}

		// Join both requests before advancing. PutChunk separately owns its HTTP
		// body bytes, so transport cleanup cannot retain our reusable buffer.
		var writeErr error
		for j := 0; j < 2; j++ {
			if err := <-results; err != nil {
				writeErr = errors.Join(writeErr, err)
			}
		}

		if writeErr != nil {
			s.cleanup(targets)
			return metadata.Object{}, fmt.Errorf(
				"%w: %s: %w",
				ErrReplicationFailed, chunkID, writeErr,
			)
		}

		// Record the successfully replicated chunk.
		chunks = append(chunks, metadata.Chunk{
			ID:       chunkID,
			Index:    index,
			Size:     int64(n),
			Checksum: hex.EncodeToString(digest[:]),
			Replicas: []string{
				replicas[0].ID, replicas[1].ID,
			},
		})

		totalSize += int64(n)

		// The last chunk can be smaller than 1 MB.
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
	}

	// Publish complete object metadata.
	now := time.Now().UTC()

	obj := metadata.Object{
		ID:             id,
		Name:           name,
		Size:           totalSize,
		ContentType:    contentType,
		CreatedAt:      now,
		ModifiedAt:     now,
		Checksum:       hex.EncodeToString(objectHash.Sum(nil)),
		ChunkCount:     len(chunks),
		Chunks:         chunks,
		CustomMetadata: custom,
	}

	if err := s.store.CreateObject(ctx, obj); err != nil {
		s.cleanup(targets)
		return metadata.Object{}, fmt.Errorf(
			"publish object: %w", err,
		)
	}

	return obj, nil
}
