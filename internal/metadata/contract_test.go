package metadata

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// backend constructs a fresh, empty Store for one subtest.
type backend struct {
	name string
	open func(t *testing.T) Store
}

func backends() []backend {
	return []backend{
		{"memory", func(t *testing.T) Store { return NewMemoryStore() }},
		{"sqlite", func(t *testing.T) Store {
			s, err := OpenSQLiteStore(filepath.Join(t.TempDir(), "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			return s
		}},
	}
}

// runContract runs fn once per backend.
func runContract(t *testing.T, fn func(t *testing.T, s Store)) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) { fn(t, b.open(t)) })
	}
}

func sampleObject(id string, chunks int) Object {
	created := time.Date(2026, 11, 19, 8, 30, 15, 123456789, time.UTC)
	obj := Object{
		ID:             id,
		Name:           id + ".bin",
		ContentType:    "application/octet-stream",
		CreatedAt:      created,
		ModifiedAt:     created.Add(time.Second),
		Checksum:       fmt.Sprintf("%064x", len(id)),
		ChunkCount:     chunks,
		Chunks:         make([]Chunk, 0, chunks),
		CustomMetadata: map[string]string{"owner": "alice", "unicode": "ναι ✓"},
	}
	workers := []string{"worker1", "worker2", "worker3", "worker4"}
	for i := 0; i < chunks; i++ {
		size := int64(1_000_000)
		if i == chunks-1 {
			size = 345_679
		}
		obj.Size += size
		obj.Chunks = append(obj.Chunks, Chunk{
			ID:       fmt.Sprintf("%s-%d", id, i),
			Index:    i,
			Size:     size,
			Checksum: fmt.Sprintf("%064x", i+1),
			// Deliberately not sorted, to check that slot order is preserved.
			Replicas: []string{workers[(i+1)%4], workers[i%4]},
		})
	}
	return obj
}

func mustGet(t *testing.T, s Store, id string) Object {
	t.Helper()
	obj, err := s.GetObject(context.Background(), id)
	if err != nil {
		t.Fatalf("GetObject(%s): %v", id, err)
	}
	return obj
}

func TestContractRoundTrip(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		for _, obj := range []Object{sampleObject("obj-multi", 3), sampleObject("obj-empty", 0)} {
			if err := s.CreateObject(ctx, obj); err != nil {
				t.Fatal(err)
			}
			got := mustGet(t, s, obj.ID)
			if !reflect.DeepEqual(got, obj) {
				t.Fatalf("round trip mismatch\n got: %+v\nwant: %+v", got, obj)
			}
			if !got.CreatedAt.Equal(obj.CreatedAt) || got.CreatedAt.Nanosecond() != 123456789 {
				t.Fatalf("timestamp precision lost: %v", got.CreatedAt)
			}
		}
		empty := mustGet(t, s, "obj-empty")
		if empty.Chunks == nil || len(empty.Chunks) != 0 || empty.CustomMetadata == nil {
			t.Fatalf("empty object should have non-nil empty chunks/metadata: %+v", empty)
		}
	})
}

func TestContractNilCustomMetadata(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		obj := sampleObject("obj", 1)
		obj.CustomMetadata = nil
		if err := s.CreateObject(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
		got := mustGet(t, s, "obj")
		if got.CustomMetadata == nil || len(got.CustomMetadata) != 0 {
			t.Fatalf("custom metadata: %#v", got.CustomMetadata)
		}
	})
}

func TestContractDuplicateAndMissing(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		obj := sampleObject("obj", 2)
		if err := s.CreateObject(ctx, obj); err != nil {
			t.Fatal(err)
		}
		dup := sampleObject("obj", 1)
		dup.Name = "other"
		if err := s.CreateObject(ctx, dup); !errors.Is(err, ErrObjectExists) {
			t.Fatalf("duplicate: %v", err)
		}
		if got := mustGet(t, s, "obj"); !reflect.DeepEqual(got, obj) {
			t.Fatal("duplicate create modified existing object")
		}
		if _, err := s.GetObject(ctx, "missing"); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("get missing: %v", err)
		}
		if _, err := s.PatchCustomMetadata(ctx, "missing", nil); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("patch missing: %v", err)
		}
		if err := s.MarkDeleting(ctx, "missing"); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("mark missing: %v", err)
		}
		if err := s.DeleteObject(ctx, "missing"); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("delete missing: %v", err)
		}
		if err := s.UpdateChunkReplicas(ctx, "missing", "c", nil, []string{"a", "b"}); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("repair missing object: %v", err)
		}
		if err := s.UpdateChunkReplicas(ctx, "obj", "nope", nil, []string{"a", "b"}); !errors.Is(err, ErrChunkNotFound) {
			t.Fatalf("repair missing chunk: %v", err)
		}
	})
}

func TestContractDetachedSnapshots(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		obj := sampleObject("obj", 2)
		if err := s.CreateObject(ctx, obj); err != nil {
			t.Fatal(err)
		}
		obj.CustomMetadata["owner"] = "external"
		obj.Chunks[0].Replicas[0] = "external"
		got := mustGet(t, s, "obj")
		if got.CustomMetadata["owner"] != "alice" || got.Chunks[0].Replicas[0] == "external" {
			t.Fatal("stored object aliases caller input")
		}
		got.CustomMetadata["owner"] = "external"
		got.Chunks[0].Replicas[0] = "external"
		again := mustGet(t, s, "obj")
		if again.CustomMetadata["owner"] != "alice" || again.Chunks[0].Replicas[0] == "external" {
			t.Fatal("GetObject result aliases store")
		}
		listed, err := s.ListObjects(ctx)
		if err != nil {
			t.Fatal(err)
		}
		listed[0].Chunks[1].Replicas[1] = "external"
		if mustGet(t, s, "obj").Chunks[1].Replicas[1] == "external" {
			t.Fatal("ListObjects result aliases store")
		}
	})
}

func TestContractListSortedAndHidesTombstones(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		for _, id := range []string{"obj-c", "obj-a", "obj-d", "obj-b"} {
			if err := s.CreateObject(ctx, sampleObject(id, 2)); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.MarkDeleting(ctx, "obj-b"); err != nil {
			t.Fatal(err)
		}
		listed, err := s.ListObjects(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, obj := range listed {
			ids = append(ids, obj.ID)
			if !reflect.DeepEqual(obj, sampleObject(obj.ID, 2)) {
				t.Fatalf("listed object %s differs from stored manifest", obj.ID)
			}
		}
		if !reflect.DeepEqual(ids, []string{"obj-a", "obj-c", "obj-d"}) {
			t.Fatalf("list order/tombstones: %v", ids)
		}
		// GetObject retains the tombstone for DELETE retry.
		if tomb := mustGet(t, s, "obj-b"); !tomb.Deleting || len(tomb.Chunks) != 2 {
			t.Fatalf("tombstone snapshot: %+v", tomb)
		}
		// MarkDeleting may be retried.
		if err := s.MarkDeleting(ctx, "obj-b"); err != nil {
			t.Fatalf("retry MarkDeleting: %v", err)
		}
	})
}

func TestContractEmptyList(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		listed, err := s.ListObjects(context.Background())
		if err != nil || listed == nil || len(listed) != 0 {
			t.Fatalf("empty list: %#v %v", listed, err)
		}
	})
}

func TestContractPatchMerges(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		obj := sampleObject("obj", 2)
		if err := s.CreateObject(ctx, obj); err != nil {
			t.Fatal(err)
		}
		before := time.Now().UTC()
		patched, err := s.PatchCustomMetadata(ctx, "obj", map[string]string{"owner": "bob", "course": "cloud"})
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]string{"owner": "bob", "course": "cloud", "unicode": "ναι ✓"}
		if !reflect.DeepEqual(patched.CustomMetadata, want) {
			t.Fatalf("merge: %v", patched.CustomMetadata)
		}
		if patched.ModifiedAt.Before(before.Add(-time.Second)) || !patched.CreatedAt.Equal(obj.CreatedAt) {
			t.Fatalf("timestamps: created=%v modified=%v", patched.CreatedAt, patched.ModifiedAt)
		}
		stored := mustGet(t, s, "obj")
		if !reflect.DeepEqual(stored, patched) {
			t.Fatal("returned patch result differs from stored object")
		}
		// Unrelated data is untouched.
		patched.CustomMetadata, patched.ModifiedAt = obj.CustomMetadata, obj.ModifiedAt
		if !reflect.DeepEqual(patched, obj) {
			t.Fatal("patch changed unrelated fields")
		}
	})
}

func TestContractTombstoneRejectsMutations(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		obj := sampleObject("obj", 1)
		if err := s.CreateObject(ctx, obj); err != nil {
			t.Fatal(err)
		}
		if err := s.MarkDeleting(ctx, "obj"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PatchCustomMetadata(ctx, "obj", map[string]string{"x": "y"}); !errors.Is(err, ErrObjectDeleting) {
			t.Fatalf("patch tombstone: %v", err)
		}
		if err := s.UpdateChunkReplicas(ctx, "obj", obj.Chunks[0].ID, obj.Chunks[0].Replicas, []string{"worker3", "worker4"}); !errors.Is(err, ErrObjectDeleting) {
			t.Fatalf("repair tombstone: %v", err)
		}
		got := mustGet(t, s, "obj")
		if got.CustomMetadata["x"] != "" || !reflect.DeepEqual(got.Chunks[0].Replicas, obj.Chunks[0].Replicas) {
			t.Fatal("rejected mutation changed tombstone")
		}
		if err := s.DeleteObject(ctx, "obj"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetObject(ctx, "obj"); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("deleted: %v", err)
		}
		// The ID can be reused after deletion, proving no rows were left behind.
		if err := s.CreateObject(ctx, obj); err != nil {
			t.Fatalf("recreate after delete: %v", err)
		}
	})
}

func TestContractReplicaCAS(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		obj := sampleObject("obj", 2)
		if err := s.CreateObject(ctx, obj); err != nil {
			t.Fatal(err)
		}
		chunk := obj.Chunks[1]
		for _, bad := range [][]string{nil, {"worker1"}, {"worker1", "worker1"}, {"a", "b", "c"}} {
			if err := s.UpdateChunkReplicas(ctx, "obj", chunk.ID, chunk.Replicas, bad); !errors.Is(err, ErrReplicaConflict) {
				t.Fatalf("invalid replacement %v: %v", bad, err)
			}
		}
		// Expected placement is ordered: a reversed slice is a conflict.
		reversed := []string{chunk.Replicas[1], chunk.Replicas[0]}
		if err := s.UpdateChunkReplicas(ctx, "obj", chunk.ID, reversed, []string{"worker3", "worker4"}); !errors.Is(err, ErrReplicaConflict) {
			t.Fatalf("reversed expected: %v", err)
		}
		if err := s.UpdateChunkReplicas(ctx, "obj", chunk.ID, chunk.Replicas, []string{"worker4", "worker3"}); err != nil {
			t.Fatal(err)
		}
		got := mustGet(t, s, "obj")
		if !reflect.DeepEqual(got.Chunks[1].Replicas, []string{"worker4", "worker3"}) {
			t.Fatalf("replacement order: %v", got.Chunks[1].Replicas)
		}
		if !reflect.DeepEqual(got.Chunks[0], obj.Chunks[0]) {
			t.Fatal("other chunk changed")
		}
		// The old expected placement is now stale.
		if err := s.UpdateChunkReplicas(ctx, "obj", chunk.ID, chunk.Replicas, []string{"worker1", "worker2"}); !errors.Is(err, ErrReplicaConflict) {
			t.Fatalf("stale expected: %v", err)
		}
	})
}

func TestContractConcurrentReplicaCAS(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		obj := sampleObject("obj", 1)
		if err := s.CreateObject(ctx, obj); err != nil {
			t.Fatal(err)
		}
		chunk := obj.Chunks[0]
		const writers = 8
		var wg sync.WaitGroup
		results := make(chan error, writers)
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results <- s.UpdateChunkReplicas(ctx, "obj", chunk.ID, chunk.Replicas,
					[]string{"worker3", fmt.Sprintf("other%d", i)})
			}(i)
		}
		wg.Wait()
		close(results)
		successes := 0
		for err := range results {
			if err == nil {
				successes++
			} else if !errors.Is(err, ErrReplicaConflict) {
				t.Fatal(err)
			}
		}
		if successes != 1 {
			t.Fatalf("CAS committed %d writers", successes)
		}
	})
}

func TestContractCanceledOperations(t *testing.T) {
	runContract(t, func(t *testing.T, s Store) {
		obj := sampleObject("obj", 2)
		if err := s.CreateObject(context.Background(), obj); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := s.CreateObject(ctx, sampleObject("new", 1)); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := s.GetObject(ctx, "obj"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := s.ListObjects(ctx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := s.PatchCustomMetadata(ctx, "obj", map[string]string{"x": "y"}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := s.MarkDeleting(ctx, "obj"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := s.UpdateChunkReplicas(ctx, "obj", obj.Chunks[0].ID, obj.Chunks[0].Replicas, []string{"a", "b"}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := s.DeleteObject(ctx, "obj"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		// Nothing changed.
		if _, err := s.GetObject(context.Background(), "new"); !errors.Is(err, ErrObjectNotFound) {
			t.Fatalf("canceled create published: %v", err)
		}
		if got := mustGet(t, s, "obj"); !reflect.DeepEqual(got, obj) {
			t.Fatal("canceled mutation changed object")
		}
	})
}
