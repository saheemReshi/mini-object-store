package controller

import (
	"context"

	"mini-object-store/internal/metadata"
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
