package metadata

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrObjectExists   = errors.New("object already exists")
	ErrObjectNotFound = errors.New("object not found")
)

type Chunk struct {
	ID       string   `json:"chunk_id"`
	Index    int      `json:"index"`
	Size     int64    `json:"size"`
	Checksum string   `json:"checksum"`
	Replicas []string `json:"replicas"`
}

type Object struct {
	ID             string            `json:"object_id"`
	Name           string            `json:"name"`
	Size           int64             `json:"size"`
	ContentType    string            `json:"content_type"`
	CreatedAt      time.Time         `json:"created_at"`
	ModifiedAt     time.Time         `json:"last_modified"`
	Checksum       string            `json:"checksum"`
	ChunkCount     int               `json:"chunk_count"`
	Chunks         []Chunk           `json:"chunks"`
	CustomMetadata map[string]string `json:"custom_metadata"`
}

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
}
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string]Object
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		objects: make(map[string]Object),
	}
}

func cloneObject(obj Object) Object {
	cloned := obj

	cloned.CustomMetadata = make(
		map[string]string, len(obj.CustomMetadata),
	)
	for k, v := range obj.CustomMetadata {
		cloned.CustomMetadata[k] = v
	}

	cloned.Chunks = make([]Chunk, len(obj.Chunks))
	copy(cloned.Chunks, obj.Chunks)

	for i := range cloned.Chunks {
		cloned.Chunks[i].Replicas = append(
			[]string(nil), obj.Chunks[i].Replicas...,
		)
	}

	return cloned
}

func (m *MemoryStore) CreateObject(
	ctx context.Context, obj Object,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.objects[obj.ID]; exists {
		return ErrObjectExists
	}

	m.objects[obj.ID] = cloneObject(obj)
	return nil
}

func (m *MemoryStore) GetObject(
	ctx context.Context, id string,
) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	obj, exists := m.objects[id]
	if !exists {
		return Object{}, ErrObjectNotFound
	}

	return cloneObject(obj), nil
}

func (m *MemoryStore) ListObjects(
	ctx context.Context,
) ([]Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Object, 0, len(m.objects))
	for _, obj := range m.objects {
		result = append(result, cloneObject(obj))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

func (m *MemoryStore) PatchCustomMetadata(
	ctx context.Context,
	id string,
	patch map[string]string,
) (Object, error) {
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	obj, exists := m.objects[id]
	if !exists {
		return Object{}, ErrObjectNotFound
	}

	obj = cloneObject(obj)

	if obj.CustomMetadata == nil {
		obj.CustomMetadata = make(map[string]string)
	}

	for key, value := range patch {
		obj.CustomMetadata[key] = value
	}

	obj.ModifiedAt = time.Now().UTC()
	m.objects[id] = obj

	return cloneObject(obj), nil
}

func (m *MemoryStore) DeleteObject(
	ctx context.Context,
	id string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.objects[id]; !exists {
		return ErrObjectNotFound
	}

	delete(m.objects, id)
	return nil
}
