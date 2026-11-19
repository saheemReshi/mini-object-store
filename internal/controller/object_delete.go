package controller

import (
	"context"
	"errors"
	"fmt"
)

var ErrDeletionIncomplete = errors.New(
	"could not remove all chunk replicas",
)

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

	// Keep the tombstone until every worker in the fixed deployment has
	// acknowledged deletion, including workers holding old untracked replicas.
	var deleteErrors []error
	if len(workers) != 4 {
		deleteErrors = append(deleteErrors, errors.New("expected four registered workers"))
	}

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
