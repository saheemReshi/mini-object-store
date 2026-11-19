package metadata

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestMemoryStoreIsolationCASAndTombstone(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	obj := Object{ID: "object", CustomMetadata: map[string]string{"owner": "alice"}, Chunks: []Chunk{{ID: "chunk", Replicas: []string{"w1", "w2"}}}}
	if err := s.CreateObject(ctx, obj); err != nil {
		t.Fatal(err)
	}
	obj.CustomMetadata["owner"] = "external"
	obj.Chunks[0].Replicas[0] = "external"
	got, err := s.GetObject(ctx, "object")
	if err != nil || got.CustomMetadata["owner"] != "alice" || got.Chunks[0].Replicas[0] != "w1" {
		t.Fatalf("input alias: %+v %v", got, err)
	}
	got.CustomMetadata["owner"] = "external"
	got.Chunks[0].Replicas[0] = "external"
	listed, _ := s.ListObjects(ctx)
	if listed[0].CustomMetadata["owner"] != "alice" || listed[0].Chunks[0].Replicas[0] != "w1" {
		t.Fatal("output alias")
	}
	if err := s.CreateObject(ctx, obj); !errors.Is(err, ErrObjectExists) {
		t.Fatalf("duplicate: %v", err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, replacement := range [][]string{{"w1", "w3"}, {"w1", "w4"}} {
		wg.Add(1)
		go func(ids []string) {
			defer wg.Done()
			results <- s.UpdateChunkReplicas(ctx, "object", "chunk", []string{"w1", "w2"}, ids)
		}(replacement)
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
	if _, err := s.PatchCustomMetadata(ctx, "object", map[string]string{"owner": "bob"}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkDeleting(ctx, "object"); err != nil {
		t.Fatal(err)
	}
	listed, _ = s.ListObjects(ctx)
	if len(listed) != 0 {
		t.Fatal("tombstone listed")
	}
	if _, err := s.PatchCustomMetadata(ctx, "object", nil); !errors.Is(err, ErrObjectDeleting) {
		t.Fatalf("patch tombstone: %v", err)
	}
	if err := s.UpdateChunkReplicas(ctx, "object", "chunk", nil, []string{"w1", "w2"}); !errors.Is(err, ErrObjectDeleting) {
		t.Fatalf("repair tombstone: %v", err)
	}
	if err := s.DeleteObject(ctx, "object"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetObject(ctx, "object"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("Get deleted object: %v", err)
	}
}

func TestCanceledStoreOperations(t *testing.T) {
	s := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.CreateObject(ctx, Object{ID: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.GetObject(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.ListObjects(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.PatchCustomMetadata(ctx, "x", nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.MarkDeleting(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.DeleteObject(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.UpdateChunkReplicas(ctx, "x", "c", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
