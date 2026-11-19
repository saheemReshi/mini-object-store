package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"mini-object-store/internal/metadata"
	"mini-object-store/internal/storage"
)

var (
	ErrInsufficientWorkers = errors.New("fewer than two healthy workers")
	ErrReplicationFailed   = errors.New("chunk replication failed")
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

type replicaTarget struct {
	address string
	chunkID string
}

// Failed cleanup is retained until a later repair pass can reach the worker.
// Like object metadata, this queue is intentionally in memory only.
func (s *ObjectService) cleanup(targets []replicaTarget) {
	s.cleanupMu.Lock()
	for _, target := range targets {
		s.pendingCleanup[target] = struct{}{}
	}
	s.cleanupMu.Unlock()
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

func (s *ObjectService) Upload(
	ctx context.Context,
	name, contentType string,
	custom map[string]string,
	src io.Reader,
) (metadata.Object, error) {

	// 1. Obtain a snapshot of healthy workers.
	healthy := make([]Worker, 0)
	for _, w := range s.registry.List() {
		if w.Status == "healthy" {
			healthy = append(healthy, w)
		}
	}

	if len(healthy) < 2 {
		return metadata.Object{}, ErrInsufficientWorkers
	}

	// 2. Generate an unpredictable unique object ID.
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

	// 3. Read the input file incrementally.
	for index := 0; ; index++ {
		if err := ctx.Err(); err != nil {
			s.cleanup(targets)
			return metadata.Object{}, err
		}
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

		// 4. Select two distinct workers.
		first := (start + index) % len(healthy)

		replicas := [2]Worker{
			healthy[first],
			healthy[(first+1)%len(healthy)],
		}

		// 5. Replicate concurrently.
		results := make(chan error, 2)

		for _, w := range replicas {
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

		// Wait for BOTH replicas to finish.
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

		// 6. Record the successfully replicated chunk.
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

	// 7. Publish complete object metadata.
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
