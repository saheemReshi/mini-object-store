package controller

import (
	"context"
	"errors"
	"fmt"

	"mini-object-store/internal/metadata"
)

var ErrDeletionIncomplete = errors.New(
	"could not remove all chunk replicas",
)

func (s *ObjectService) GetMetadata(
	ctx context.Context,
	id string,
) (metadata.Object, error) {
	unlock := s.locks.lock(id, false)
	defer unlock()

	return s.store.GetObject(ctx, id)
}

func (s *ObjectService) ListObjects(
	ctx context.Context,
) ([]metadata.Object, error) {
	return s.store.ListObjects(ctx)
}

func (s *ObjectService) PatchMetadata(
	ctx context.Context,
	id string,
	patch map[string]string,
) (metadata.Object, error) {
	unlock := s.locks.lock(id, true)
	defer unlock()

	return s.store.PatchCustomMetadata(ctx, id, patch)
}

func (s *ObjectService) DeleteObject(
	ctx context.Context,
	id string,
) error {
	unlock := s.locks.lock(id, true)
	defer unlock()

	obj, err := s.store.GetObject(ctx, id)
	if err != nil {
		return err
	}

	workers := make(map[string]string)
	for _, w := range s.registry.List() {
		workers[w.ID] = w.Address
	}

	var deleteErrors []error

	// Remove all physical replicas before deleting metadata.
	for _, chunk := range obj.Chunks {
		for _, workerID := range chunk.Replicas {
			address, exists := workers[workerID]
			if !exists {
				deleteErrors = append(deleteErrors,
					fmt.Errorf("unknown worker %s", workerID))
				continue
			}

			err := s.client.DeleteChunk(
				ctx, address, chunk.ID,
			)
			if err != nil {
				deleteErrors = append(deleteErrors,
					fmt.Errorf("delete %s on %s: %w",
						chunk.ID, workerID, err))
			}
		}
	}

	if len(deleteErrors) > 0 {
		return fmt.Errorf("%w: %w",
			ErrDeletionIncomplete,
			errors.Join(deleteErrors...))
	}

	// Only now remove the logical object record.
	return s.store.DeleteObject(ctx, id)
}
