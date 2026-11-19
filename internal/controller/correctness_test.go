package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"mini-object-store/internal/metadata"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPutOwnsBodyAfterTransportReturns(t *testing.T) {
	var retained io.ReadCloser
	c := NewWorkerClient()
	c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		retained = r.Body
		return &http.Response{StatusCode: 503, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
	})
	data := []byte("the original chunk")
	want := bytes.Clone(data)
	if err := c.PutChunk(context.Background(), "http://worker", "chunk", data, sha256.Sum256(data)); err == nil {
		t.Fatal("expected HTTP error")
	}
	copy(data, bytes.Repeat([]byte{'x'}, len(data)))
	got, err := io.ReadAll(retained)
	retained.Close()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("request body aliased caller's reused buffer: %q, %v", got, err)
	}
}

func TestWorkerClientCancellationAndRedirect(t *testing.T) {
	c := NewWorkerClient()
	c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.GetChunk(ctx, "http://worker", "chunk", 1, sha256.Sum256([]byte("x"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get canceled: %v", err)
	}
	if _, err := c.GetChunk(context.Background(), "http://worker", "chunk", -1, [32]byte{}); !errors.Is(err, ErrChunkCorrupt) {
		t.Fatalf("invalid expected size: %v", err)
	}
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("worker redirect followed"); w.WriteHeader(201) }))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	err := NewWorkerClient().PutChunk(context.Background(), source.URL, "chunk", []byte("x"), sha256.Sum256([]byte("x")))
	var status *HTTPStatusError
	if !errors.As(err, &status) || status.Code != 307 {
		t.Fatalf("redirect accepted: %v", err)
	}
}

func TestWorkerRegistryEndpointUniquenessAndSnapshot(t *testing.T) {
	r := NewRegistry()
	if _, _, err := r.Register("w1", "http://localhost:8001"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Register("w2", "http://localhost:8001"); !errors.Is(err, ErrWorkerConflict) {
		t.Fatalf("duplicate endpoint: %v", err)
	}
	if err := r.Heartbeat("w1"); err != nil {
		t.Fatal(err)
	}
	w := r.List()[0]
	*w.LastHeartbeat = time.Time{}
	if r.List()[0].LastHeartbeat.IsZero() {
		t.Fatal("registry exposed its heartbeat pointer")
	}
}

func TestObjectLockWaiterLifecycle(t *testing.T) {
	table := newObjectLockTable()
	release := table.lock("a", false)
	done := make(chan struct{})
	go func() { unlock := table.lock("a", true); unlock(); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		table.mu.Lock()
		refs := table.entries["a"].refs
		table.mu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			release()
			t.Fatal("waiting writer not counted")
		}
		time.Sleep(time.Millisecond)
	}
	// A different object must proceed independently.
	other := table.lock("b", true)
	other()
	release()
	<-done
	if len(table.entries) != 0 {
		t.Fatal("lock table leaked")
	}
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				unlock := table.lock("a", true)
				unlock()
			}
		}()
	}
	wg.Wait()
	if len(table.entries) != 0 {
		t.Fatal("lock table leaked under contention")
	}
}

type failingReader struct{ data *bytes.Reader }

func (r failingReader) Read(p []byte) (int, error) {
	if r.data.Len() == 0 {
		return 0, errors.New("injected read failure")
	}
	return r.data.Read(p)
}

func TestUploadReadFailureAndCancellation(t *testing.T) {
	c := newTestCluster(t)
	_, err := c.service.Upload(context.Background(), "broken", "", nil, failingReader{bytes.NewReader(bytes.Repeat([]byte{1}, 1_000_000))})
	if err == nil {
		t.Fatal("read failure ignored")
	}
	c.checkEmpty(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.service.Upload(ctx, "canceled", "", nil, bytes.NewReader(nil)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled empty upload: %v", err)
	}
	objects, _ := c.meta.ListObjects(context.Background())
	if len(objects) != 0 {
		t.Fatal("failed upload published metadata")
	}
}

func TestTransferPreservesCancellation(t *testing.T) {
	c := newTestCluster(t)
	obj := c.upload(t, []byte("cancel during transfer"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.service.client.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		cancel()
		return nil, context.Canceled
	})
	if _, _, err := c.service.Download(ctx, obj.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("download lost cancellation: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c.service.client.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == "DELETE" {
			return &http.Response{StatusCode: 404, Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}, nil
		}
		cancel2()
		return nil, context.Canceled
	})
	_, err := c.service.Upload(ctx2, "cancel", "", nil, bytes.NewReader([]byte("cancel me")))
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrReplicationFailed) {
		t.Fatalf("upload lost error cause: %v", err)
	}
}

func TestDownloadRejectsCorruptMetadataAndAllBadReplicas(t *testing.T) {
	c := newTestCluster(t)
	data := []byte("integrity")
	obj := c.upload(t, data)
	for _, id := range obj.Chunks[0].Replicas {
		if err := os.WriteFile(filepath.Join(c.workers[id].dir, obj.Chunks[0].ID), []byte("bad"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := downloadBytes(c.service, obj.ID); !errors.Is(err, ErrChunkUnavailable) {
		t.Fatalf("all corrupt: %v", err)
	}
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	obj.ID = "invalid-order"
	obj.Chunks[0].Index = 1
	if err := c.meta.CreateObject(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	if _, err := downloadBytes(c.service, obj.ID); !errors.Is(err, ErrObjectCorrupt) {
		t.Fatalf("order: %v", err)
	}
	obj.ID = "invalid-hash"
	obj.Chunks[0].Index = 0
	obj.Chunks[0].Checksum = "xyz"
	if err := c.meta.CreateObject(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	if _, err := downloadBytes(c.service, obj.ID); !errors.Is(err, ErrObjectCorrupt) {
		t.Fatalf("chunk hash: %v", err)
	}
	obj.ID = "invalid-count"
	obj.ChunkCount = 2
	if err := c.meta.CreateObject(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	if _, err := downloadBytes(c.service, obj.ID); !errors.Is(err, ErrObjectCorrupt) {
		t.Fatalf("chunk count: %v", err)
	}
	// Verify the complete-object checksum, including objects without chunks.
	if err := c.meta.CreateObject(context.Background(), metadata.Object{ID: "bad-empty", Checksum: "wrong"}); err != nil {
		t.Fatal(err)
	}
	if _, err := downloadBytes(c.service, "bad-empty"); !errors.Is(err, ErrObjectCorrupt) {
		t.Fatalf("object hash: %v", err)
	}
}
