package controller

import (
	"context"

	"mini-object-store/internal/metadata"
)

// GetMetadata returns a detached snapshot of an object that is not being deleted.
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

// ListObjects excludes deletion tombstones through the Store contract.
func (s *ObjectService) ListObjects(
	ctx context.Context,
) ([]metadata.Object, error) {
	return s.store.ListObjects(ctx)
}

// PatchMetadata merges custom key/value pairs without changing stored object data.
func (s *ObjectService) PatchMetadata(
	ctx context.Context,
	id string,
	patch map[string]string,
) (metadata.Object, error) {
	unlock := s.locks.lock(id, true)
	defer unlock()

	return s.store.PatchCustomMetadata(ctx, id, patch)
}
