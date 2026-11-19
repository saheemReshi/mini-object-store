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

	obj, err := s.store.GetObject(ctx, id)
	if err != nil {
		return metadata.Object{}, err
	}

	if obj.Deleting {
		return metadata.Object{}, metadata.ErrObjectDeleting
	}

	return obj, nil
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

	// Persist deletion intent BEFORE removing physical chunks.
	if err := s.store.MarkDeleting(ctx, id); err != nil {
		return err
	}

	workers := s.registry.List()

	// Strict policy for our fixed four-worker deployment:
	// require all workers to be reachable before deletion.
	if len(workers) != 4 {
		return fmt.Errorf(
			"%w: expected four registered workers",
			ErrDeletionIncomplete,
		)
	}

	for _, w := range workers {
		if w.Status != "healthy" {
			return fmt.Errorf(
				"%w: worker %s unavailable",
				ErrDeletionIncomplete, w.ID,
			)
		}
	}

	var deleteErrors []error

	// Delete tracked and untracked copies on ALL workers.
	for _, chunk := range obj.Chunks {
		for _, w := range workers {
			err := s.client.DeleteChunk(
				ctx,
				w.Address,
				chunk.ID,
			)

			if err != nil {
				deleteErrors = append(deleteErrors,
					fmt.Errorf(
						"delete %s on %s: %w",
						chunk.ID, w.ID, err,
					),
				)
			}
		}
	}

	if len(deleteErrors) > 0 {
		return fmt.Errorf(
			"%w: %w",
			ErrDeletionIncomplete,
			errors.Join(deleteErrors...),
		)
	}

	// All physical deletions succeeded (or returned 404).
	return s.store.DeleteObject(ctx, id)
}
