package metadata

import (
	"context"
	"errors"
)

var (
	ErrObjectExists    = errors.New("object already exists")
	ErrObjectNotFound  = errors.New("object not found")
	ErrObjectDeleting  = errors.New("object deletion in progress")
	ErrReplicaConflict = errors.New("replica locations changed")
	ErrChunkNotFound   = errors.New("chunk not found in metadata")
)

// Store implementations must return detached snapshots and make each mutation
// atomic: a returned error must leave metadata unchanged. Replica updates compare
// the expected ordered placement and reject deleting objects in the same atomic
// operation. ListObjects hides tombstones; GetObject retains them for DELETE retry.
// These semantics also apply to the future SQLite implementation.
type Store interface {
	CreateObject(ctx context.Context, obj Object) error
	GetObject(ctx context.Context, id string) (Object, error)

	ListObjects(ctx context.Context) ([]Object, error)
	PatchCustomMetadata(
		ctx context.Context,
		id string,
		patch map[string]string,
	) (Object, error)
	DeleteObject(ctx context.Context, id string) error
	MarkDeleting(
		ctx context.Context,
		objectID string,
	) error

	UpdateChunkReplicas(
		ctx context.Context,
		objectID string,
		chunkID string,
		expected []string,
		replacement []string,
	) error
}
