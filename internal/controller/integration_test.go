package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mini-object-store/internal/metadata"
	"mini-object-store/internal/storage"
	"mini-object-store/internal/worker"
)

const (
	failGet uint32 = 1 << iota
	failDelete
	losePutResponse
)

type testWorker struct {
	id, dir, address string
	faults           atomic.Uint32
}

type testCluster struct {
	registry *Registry
	meta     *metadata.MemoryStore
	service  *ObjectService
	workers  map[string]*testWorker
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	c := &testCluster{registry: NewRegistry(), meta: metadata.NewMemoryStore(), workers: make(map[string]*testWorker)}
	c.service = NewObjectService(c.registry, c.meta, NewWorkerClient())
	api := httptest.NewServer(NewHandler(c.registry, c.service))
	t.Cleanup(api.Close)
	for i := 0; i < 4; i++ {
		w := &testWorker{id: fmt.Sprintf("worker%d", i), dir: t.TempDir()}
		fs, err := storage.NewFileStore(w.dir)
		if err != nil {
			t.Fatal(err)
		}
		h := worker.NewHandler(fs, w.id)
		srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			faults := w.faults.Load()
			if (r.Method == "GET" && faults&failGet != 0) || (r.Method == "DELETE" && faults&failDelete != 0) {
				http.Error(rw, "injected unavailability", 503)
				return
			}
			if r.Method == "PUT" && faults&losePutResponse != 0 {
				h.ServeHTTP(httptest.NewRecorder(), r)
				http.Error(rw, "injected lost acknowledgement", 503)
				return
			}
			h.ServeHTTP(rw, r)
		}))
		t.Cleanup(srv.Close)
		w.address = srv.URL
		c.workers[w.id] = w
		if err := worker.Register(context.Background(), api.Client(), api.URL, w.id, srv.URL); err != nil {
			t.Fatal(err)
		}
		if err := worker.Heartbeat(context.Background(), api.Client(), api.URL, w.id); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func (c *testCluster) upload(t *testing.T, data []byte) metadata.Object {
	t.Helper()
	obj, err := c.service.Upload(context.Background(), "random.bin", "application/octet-stream", map[string]string{"owner": "alice"}, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func (c *testCluster) expire(id string) {
	c.registry.mu.Lock()
	w := c.registry.workers[id]
	w.LastContact = time.Now().Add(-WorkerFailureTimeout - time.Second)
	c.registry.workers[id] = w
	c.registry.mu.Unlock()
}

func downloadBytes(s *ObjectService, id string) ([]byte, error) {
	f, _, err := s.Download(context.Background(), id)
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	return io.ReadAll(f)
}

func (c *testCluster) checkDownload(t *testing.T, obj metadata.Object, want []byte) {
	t.Helper()
	got, err := downloadBytes(c.service, obj.ID)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("download: err=%v, got %d bytes, want %d", err, len(got), len(want))
	}
}

func (c *testCluster) checkReplicas(t *testing.T, obj metadata.Object, data []byte) {
	t.Helper()
	for _, chunk := range obj.Chunks {
		if len(chunk.Replicas) != 2 || chunk.Replicas[0] == chunk.Replicas[1] {
			t.Fatalf("invalid placement: %+v", chunk)
		}
		start := int64(chunk.Index) * storage.ChunkSize
		want := data[start : start+chunk.Size]
		copies := 0
		for id, w := range c.workers {
			b, err := os.ReadFile(filepath.Join(w.dir, chunk.ID))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			copies++
			if !containsWorker(chunk.Replicas, id) || !bytes.Equal(b, want) {
				t.Fatalf("invalid physical replica %s/%s", id, chunk.ID)
			}
		}
		if copies != 2 {
			t.Fatalf("%s has %d physical copies", chunk.ID, copies)
		}
	}
}

func (c *testCluster) checkEmpty(t *testing.T) {
	t.Helper()
	for id, w := range c.workers {
		entries, err := os.ReadDir(w.dir)
		if err != nil || len(entries) != 0 {
			t.Errorf("worker %s: %v, remaining files %v", id, err, entries)
		}
	}
}

func TestObjectSizesAndDeletion(t *testing.T) {
	c := newTestCluster(t)
	for _, size := range []int{0, 31, 1_000_000, 2_000_097} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := make([]byte, size)
			_, _ = rand.New(rand.NewSource(int64(size))).Read(data)
			obj := c.upload(t, data)
			if obj.ChunkCount != (size+999_999)/1_000_000 || obj.Checksum != fmt.Sprintf("%x", sha256.Sum256(data)) {
				t.Fatalf("bad metadata: %+v", obj)
			}
			for i, ch := range obj.Chunks {
				wantSize := min(int64(size)-int64(i)*storage.ChunkSize, storage.ChunkSize)
				if ch.Index != i || ch.Size != wantSize {
					t.Fatalf("bad chunk: %+v", ch)
				}
			}
			c.checkReplicas(t, obj, data)
			c.checkDownload(t, obj, data)
			if err := c.service.DeleteObject(context.Background(), obj.ID); err != nil {
				t.Fatal(err)
			}
			c.checkEmpty(t)
			if err := c.service.DeleteObject(context.Background(), obj.ID); !errors.Is(err, metadata.ErrObjectNotFound) {
				t.Fatalf("repeated deletion: %v", err)
			}
		})
	}
}

func TestFailureRepairRestartAndCorruption(t *testing.T) {
	c := newTestCluster(t)
	data := make([]byte, 2_000_137)
	_, _ = rand.New(rand.NewSource(42)).Read(data)
	obj := c.upload(t, data)
	victim := obj.Chunks[0].Replicas[0]
	c.workers[victim].faults.Store(failGet | failDelete)
	c.expire(victim)
	c.checkDownload(t, obj, data)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.service.RunRepair(ctx, 5*time.Millisecond) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		updated, err := c.meta.GetObject(context.Background(), obj.ID)
		if err != nil {
			t.Fatal(err)
		}
		repaired := true
		for _, ch := range updated.Chunks {
			if containsWorker(ch.Replicas, victim) {
				repaired = false
			}
		}
		if repaired {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("background repair did not replace failed worker")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	c.workers[victim].faults.Store(0)
	if _, _, err := c.registry.Register(victim, c.workers[victim].address); err != nil {
		t.Fatal(err)
	}
	if err := c.registry.Heartbeat(victim); err != nil {
		t.Fatal(err)
	}
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	obj, _ = c.meta.GetObject(context.Background(), obj.ID)
	c.checkReplicas(t, obj, data)
	c.checkDownload(t, obj, data)
	ch := obj.Chunks[0]
	if err := os.WriteFile(filepath.Join(c.workers[ch.Replicas[0]].dir, ch.ID), bytes.Repeat([]byte{9}, int(ch.Size)), 0600); err != nil {
		t.Fatal(err)
	}
	c.checkDownload(t, obj, data)
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	obj, _ = c.meta.GetObject(context.Background(), obj.ID)
	c.checkReplicas(t, obj, data)
	if err := c.service.DeleteObject(context.Background(), obj.ID); err != nil {
		t.Fatal(err)
	}
	c.checkEmpty(t)
}

func TestRepairWithOnlyTwoHealthyWorkers(t *testing.T) {
	c := newTestCluster(t)
	data := []byte("repair without a spare")
	obj := c.upload(t, data)
	ch := obj.Chunks[0]
	for id := range c.workers {
		if !containsWorker(ch.Replicas, id) {
			c.expire(id)
		}
	}
	if err := os.WriteFile(filepath.Join(c.workers[ch.Replicas[0]].dir, ch.ID), []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	obj, _ = c.meta.GetObject(context.Background(), obj.ID)
	c.checkReplicas(t, obj, data)
	c.checkDownload(t, obj, data)
}

func TestFailedUploadCleanupRetry(t *testing.T) {
	c := newTestCluster(t)
	for _, w := range c.workers {
		w.faults.Store(losePutResponse | failDelete)
	}
	_, err := c.service.Upload(context.Background(), "failed", "", nil, bytes.NewReader([]byte("published but acknowledgement lost")))
	if !errors.Is(err, ErrReplicationFailed) {
		t.Fatalf("upload: %v", err)
	}
	objects, _ := c.meta.ListObjects(context.Background())
	if len(objects) != 0 || len(c.service.pendingCleanup) != 2 {
		t.Fatal("failed upload not retained for cleanup")
	}
	for _, w := range c.workers {
		w.faults.Store(0)
	}
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.service.pendingCleanup) != 0 {
		t.Fatal("cleanup not acknowledged")
	}
	c.checkEmpty(t)
	obj := c.upload(t, []byte("retry"))
	c.checkDownload(t, obj, []byte("retry"))
}

func TestIncompleteDeletionRetry(t *testing.T) {
	c := newTestCluster(t)
	obj := c.upload(t, []byte("delete me"))
	victim := obj.Chunks[0].Replicas[0]
	c.workers[victim].faults.Store(failDelete)
	c.expire(victim)
	if err := c.service.DeleteObject(context.Background(), obj.ID); !errors.Is(err, ErrDeletionIncomplete) {
		t.Fatalf("delete: %v", err)
	}
	stored, _ := c.meta.GetObject(context.Background(), obj.ID)
	listed, _ := c.service.ListObjects(context.Background())
	if !stored.Deleting || len(listed) != 0 {
		t.Fatal("tombstone missing or exposed in listing")
	}
	if _, err := downloadBytes(c.service, obj.ID); !errors.Is(err, metadata.ErrObjectDeleting) {
		t.Fatalf("download tombstone: %v", err)
	}
	if _, err := c.service.PatchMetadata(context.Background(), obj.ID, map[string]string{"x": "y"}); !errors.Is(err, metadata.ErrObjectDeleting) {
		t.Fatalf("patch tombstone: %v", err)
	}
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for id, w := range c.workers {
		if id == victim {
			continue
		}
		if _, err := os.Stat(filepath.Join(w.dir, obj.Chunks[0].ID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("reachable replica not deleted")
		}
	}
	c.workers[victim].faults.Store(0)
	if err := c.service.DeleteObject(context.Background(), obj.ID); err != nil {
		t.Fatal(err)
	}
	c.checkEmpty(t)
}

type failingStore struct {
	metadata.Store
	failCreate, failUpdate bool
}

func (s *failingStore) CreateObject(ctx context.Context, obj metadata.Object) error {
	if s.failCreate {
		return errors.New("injected create failure")
	}
	return s.Store.CreateObject(ctx, obj)
}

func (s *failingStore) UpdateChunkReplicas(ctx context.Context, objectID, chunkID string, expected, replacement []string) error {
	if s.failUpdate {
		return errors.New("injected update failure")
	}
	return s.Store.UpdateChunkReplicas(ctx, objectID, chunkID, expected, replacement)
}

func TestMetadataPublicationFailures(t *testing.T) {
	c := newTestCluster(t)
	fs := &failingStore{Store: c.meta, failCreate: true}
	c.service.store = fs
	if _, err := c.service.Upload(context.Background(), "x", "", nil, bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("create failure ignored")
	}
	c.checkEmpty(t)
	fs.failCreate = false
	data := []byte("replica rollback")
	obj := c.upload(t, data)
	ch := obj.Chunks[0]
	if err := os.Remove(filepath.Join(c.workers[ch.Replicas[0]].dir, ch.ID)); err != nil {
		t.Fatal(err)
	}
	fs.failUpdate = true
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, _ := c.meta.GetObject(context.Background(), obj.ID)
	if !reflect.DeepEqual(stored.Chunks, obj.Chunks) {
		t.Fatal("failed repair changed metadata")
	}
	for id, w := range c.workers {
		if containsWorker(ch.Replicas, id) {
			continue
		}
		if _, err := os.Stat(filepath.Join(w.dir, ch.ID)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("failed repair leaked new copy")
		}
	}
	fs.failUpdate = false
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored, _ = c.meta.GetObject(context.Background(), obj.ID)
	c.checkReplicas(t, stored, data)
}

func TestConcurrentDownloadDeleteRepair(t *testing.T) {
	c := newTestCluster(t)
	data := bytes.Repeat([]byte("concurrent"), 120_000)
	obj := c.upload(t, data)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			b, err := downloadBytes(c.service, obj.ID)
			if err == nil && !bytes.Equal(b, data) {
				t.Error("concurrent download corrupted")
			}
			if err != nil && !errors.Is(err, metadata.ErrObjectNotFound) && !errors.Is(err, metadata.ErrObjectDeleting) {
				t.Errorf("download: %v", err)
			}
		}()
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		if err := c.service.RepairOnce(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		if err := c.service.DeleteObject(context.Background(), obj.ID); err != nil {
			t.Error(err)
		}
	}()
	close(start)
	wg.Wait()
	if err := c.service.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.checkEmpty(t)
	if len(c.service.locks.entries) != 0 {
		t.Fatal("object lock registry leaked")
	}
}
