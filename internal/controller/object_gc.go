package controller

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"mini-object-store/internal/metadata"
	"mini-object-store/internal/storage"
)

// GCOptions describes one explicitly requested startup maintenance pass.
// It does not schedule future passes or change worker membership behavior.
type GCOptions struct {
	Mode              string
	GracePeriod       time.Duration
	WorkerWaitTimeout time.Duration
}

// GCStats counts physical replicas. Bytes are inventory sizes, not filesystem
// allocation measurements; a worker's 404 is absence, never reclaimed storage.
type GCStats struct {
	Scanned, Candidates, Preserved, Unknown, Deleted, AlreadyAbsent, Deferred int64
	EstimatedDeletedBytes                                                     int64
}

// CollectOrphans excludes object operations and repair for a single maintenance
// pass. The caller must gate client HTTP requests before starting this method.
// Only an explicit not-found from persistent SQLite can authorize reclamation.
func (s *ObjectService) CollectOrphans(ctx context.Context, options GCOptions) (stats GCStats, err error) {
	started := time.Now()
	defer func() {
		log.Printf("gc: mode=%s scanned=%d candidates=%d preserved=%d unknown=%d deleted=%d estimated_deleted_bytes=%d already_absent=%d deferred=%d duration=%s error=%v",
			options.Mode, stats.Scanned, stats.Candidates, stats.Preserved, stats.Unknown,
			stats.Deleted, stats.EstimatedDeletedBytes, stats.AlreadyAbsent, stats.Deferred, time.Since(started), err)
	}()
	if (options.Mode != "delete" && options.Mode != "dry-run") || options.GracePeriod <= 0 || options.WorkerWaitTimeout <= 0 {
		return stats, errors.New("invalid GC options")
	}
	if _, ok := s.store.(*metadata.SQLiteStore); !ok {
		return stats, errors.New("GC requires persistent SQLite metadata")
	}

	// TryLock plus a cancellable wait avoids an uninterruptible maintenance
	// waiter behind a slow direct service caller. Startup normally has none.
	for !s.maintenance.TryLock() {
		if err := gcWait(ctx, 10*time.Millisecond); err != nil {
			return stats, err
		}
	}
	defer s.maintenance.Unlock()
	if _, err := s.store.ListObjects(ctx); err != nil {
		return stats, fmt.Errorf("GC metadata check: %w", err)
	}
	workers, err := s.gcWorkers(ctx, options.WorkerWaitTimeout)
	if err != nil {
		return stats, err
	}

	type candidate struct {
		worker Worker
		chunk  storage.ChunkInfo
		owner  string
	}
	candidates := make(map[replicaTarget]candidate)
	for _, w := range workers {
		inventory, err := s.gcInventory(ctx, w)
		if err != nil {
			return stats, err
		}
		for _, chunk := range inventory.Chunks {
			stats.Scanned++
			owner, ok := gcChunkOwner(chunk.ID)
			if !ok {
				stats.Unknown++
				continue
			}
			exists, err := s.gcOwnerExists(ctx, owner)
			if err != nil {
				return stats, err
			}
			if exists {
				stats.Preserved++
				continue
			}
			candidates[replicaTarget{w.Address, chunk.ID}] = candidate{w, chunk, owner}
			stats.Candidates++
		}
	}
	log.Printf("gc: mode=%s discovered=%d; grace period %s", options.Mode, stats.Candidates, options.GracePeriod)
	if err := gcWait(ctx, options.GracePeriod); err != nil {
		return stats, err
	}

	// Refresh every worker before deleting anything. Newly observed or changed
	// orphan files have not received a full grace period and require a retry.
	present := make(map[replicaTarget]storage.ChunkInfo)
	for _, w := range workers {
		inventory, err := s.gcInventory(ctx, w)
		if err != nil {
			return stats, err
		}
		for _, chunk := range inventory.Chunks {
			owner, ok := gcChunkOwner(chunk.ID)
			if !ok {
				continue
			}
			key := replicaTarget{w.Address, chunk.ID}
			present[key] = chunk
			old, found := candidates[key]
			if found && old.chunk.Size == chunk.Size {
				continue
			}
			exists, err := s.gcOwnerExists(ctx, owner)
			if err != nil {
				return stats, err
			}
			if !exists {
				stats.Deferred++
			}
		}
	}
	if stats.Deferred != 0 {
		return stats, errors.New("new or changed orphan chunks need a fresh grace period; request retained")
	}
	for key, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if _, ok := present[key]; !ok {
			stats.AlreadyAbsent++
			continue
		}
		if err := s.gcCheckWorkers(workers); err != nil {
			return stats, err
		}
		// Lock ordering is maintenance -> object -> worker chunk. Preserve every
		// chunk owned by an existing object, even untracked extra replicas and
		// incomplete deletions hidden by ListObjects.
		unlock := s.locks.lock(candidate.owner, true)
		exists, checkErr := s.gcOwnerExists(ctx, candidate.owner)
		if checkErr != nil || exists {
			unlock()
			if checkErr != nil {
				return stats, checkErr
			}
			stats.Preserved++
			continue
		}
		if options.Mode == "dry-run" {
			log.Printf("gc: would delete worker=%s chunk=%s estimated_bytes=%d", candidate.worker.ID, key.chunkID, candidate.chunk.Size)
			unlock()
			continue
		}
		deleted, deleteErr := s.client.deleteChunk(ctx, key.address, key.chunkID)
		unlock()
		if deleteErr != nil {
			return stats, fmt.Errorf("GC delete %s/%s: %w", candidate.worker.ID, key.chunkID, deleteErr)
		}
		if deleted {
			stats.Deleted++
			stats.EstimatedDeletedBytes += candidate.chunk.Size
			log.Printf("gc: deleted worker=%s chunk=%s estimated_bytes=%d", candidate.worker.ID, key.chunkID, candidate.chunk.Size)
		} else {
			stats.AlreadyAbsent++
		}
	}
	if _, err := s.store.ListObjects(ctx); err != nil {
		return stats, fmt.Errorf("GC final metadata check: %w", err)
	}
	return stats, ctx.Err()
}

func (s *ObjectService) gcOwnerExists(ctx context.Context, id string) (bool, error) {
	_, err := s.store.GetObject(ctx, id)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, metadata.ErrObjectNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("GC metadata lookup %s: %w", id, err)
}

func (s *ObjectService) gcInventory(ctx context.Context, w Worker) (WorkerInventory, error) {
	if err := s.gcCheckWorkers([]Worker{w}); err != nil {
		return WorkerInventory{}, err
	}
	inventory, err := s.client.ListChunks(ctx, w.Address, w.ID)
	if err != nil {
		return inventory, fmt.Errorf("GC inventory %s: %w", w.ID, err)
	}
	if inventory.Chunks == nil {
		return inventory, fmt.Errorf("GC inventory %s: missing chunks array", w.ID)
	}
	seen := make(map[string]bool)
	for _, ch := range inventory.Chunks {
		if ch.ID == "" || seen[ch.ID] || ch.Size <= 0 || ch.Size > storage.ChunkSize {
			return inventory, fmt.Errorf("GC inventory %s: invalid chunk entry", w.ID)
		}
		seen[ch.ID] = true
	}
	return inventory, nil
}

func (s *ObjectService) gcWorkers(ctx context.Context, timeout time.Duration) ([]Worker, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		workers := s.registry.List()
		if len(workers) == 4 && s.gcCheckWorkers(workers) == nil {
			return workers, nil
		}
		if err := gcWait(ctx, 25*time.Millisecond); err != nil {
			return nil, fmt.Errorf("GC waiting for four healthy workers: %w", err)
		}
	}
}

func (s *ObjectService) gcCheckWorkers(expected []Worker) error {
	current := s.registry.List()
	if len(current) != 4 {
		return errors.New("GC requires exactly four registered workers")
	}
	for _, want := range expected {
		found := false
		for _, got := range current {
			if got.ID == want.ID && got.Address == want.Address && got.Status == "healthy" {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("GC worker %s unavailable or changed", want.ID)
		}
	}
	return nil
}

func gcWait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

// Only Upload's canonical obj-<32 lowercase hex>-<chunk index> names are
// eligible. Unknown/legacy filenames are never guessed or erased.
func gcChunkOwner(id string) (string, bool) {
	if len(id) < 38 || !strings.HasPrefix(id, "obj-") || id[36] != '-' {
		return "", false
	}
	owner, suffix := id[:36], id[37:]
	if _, err := hex.DecodeString(owner[4:]); err != nil || strings.ToLower(owner) != owner {
		return "", false
	}
	index, err := strconv.ParseUint(suffix, 10, 63)
	return owner, err == nil && strconv.FormatUint(index, 10) == suffix
}
