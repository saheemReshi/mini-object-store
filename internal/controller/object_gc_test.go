package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mini-object-store/internal/metadata"
)

const gcOrphan = "obj-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-0"

func newGCCluster(t *testing.T) (*testCluster, *metadata.SQLiteStore) {
	t.Helper()
	c := newTestCluster(t)
	db, err := metadata.OpenSQLiteStore(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c.service.store = db
	return c, db
}

func gcTestOptions() GCOptions {
	return GCOptions{Mode: "delete", GracePeriod: time.Millisecond, WorkerWaitTimeout: time.Second}
}

func seedGCChunk(t *testing.T, c *testCluster, id string) {
	t.Helper()
	data := []byte("orphan data")
	for _, w := range c.workers {
		if err := NewWorkerClient().PutChunk(context.Background(), w.address, id, data, sha256.Sum256(data)); err != nil {
			t.Fatal(err)
		}
	}
}

func assertGCChunk(t *testing.T, c *testCluster, id string, exists bool) {
	t.Helper()
	for _, w := range c.workers {
		_, err := os.Stat(filepath.Join(w.dir, id))
		if exists && err != nil || !exists && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s/%s: expected exists=%v, err=%v", w.id, id, exists, err)
		}
	}
}

type gcRoundTripper func(*http.Request) (*http.Response, error)

func (f gcRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// The fifth inventory request is the first refreshed inventory, so this barrier
// forces changes after candidate discovery and after the full grace period.
func gcRefreshBarrier(c *testCluster) (<-chan struct{}, func()) {
	reached, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var inventories atomic.Int32
	c.service.client.http.Transport = gcRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/chunks") && inventories.Add(1) == 5 {
			close(reached)
			select {
			case <-resume:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	return reached, func() { once.Do(func() { close(resume) }) }
}

func awaitGCSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("GC barrier timed out")
	}
}

func TestGCCollectsOnlyOrphans(t *testing.T) {
	c, db := newGCCluster(t)
	live := c.upload(t, []byte("keep live"))
	tombstone := c.upload(t, []byte("keep pending deletion"))
	if err := db.MarkDeleting(context.Background(), tombstone.ID); err != nil {
		t.Fatal(err)
	}
	seedGCChunk(t, c, gcOrphan)
	seedGCChunk(t, c, "legacy-chunk")
	// GC must also preserve untracked chunks owned by a live object. Repair
	// remains responsible for reconciling replicas of existing manifest chunks.
	extra := live.ID + "-99"
	seedGCChunk(t, c, extra)
	options := gcTestOptions()
	options.Mode = "dry-run"
	stats, err := c.service.CollectOrphans(context.Background(), options)
	if err != nil || stats.Candidates != 4 || stats.Deleted != 0 || stats.Unknown != 4 {
		t.Fatalf("dry-run: %+v %v", stats, err)
	}
	assertGCChunk(t, c, gcOrphan, true)
	options.Mode = "delete"
	stats, err = c.service.CollectOrphans(context.Background(), options)
	if err != nil || stats.Deleted != 4 || stats.EstimatedDeletedBytes != 4*int64(len("orphan data")) {
		t.Fatalf("delete: %+v %v", stats, err)
	}
	assertGCChunk(t, c, gcOrphan, false)
	assertGCChunk(t, c, "legacy-chunk", true)
	assertGCChunk(t, c, extra, true)
	c.checkDownload(t, live, []byte("keep live"))
	for _, ch := range tombstone.Chunks {
		for _, id := range ch.Replicas {
			if _, err := os.Stat(filepath.Join(c.workers[id].dir, ch.ID)); err != nil {
				t.Fatal("GC removed a tombstoned replica", err)
			}
		}
	}
	stats, err = c.service.CollectOrphans(context.Background(), options)
	if err != nil || stats.Deleted != 0 {
		t.Fatalf("repeat: %+v %v", stats, err)
	}
	if len(c.service.locks.entries) != 0 {
		t.Fatal("GC leaked object lock references")
	}
}

func TestGCRechecksMetadataBeforeDeletion(t *testing.T) {
	c, db := newGCCluster(t)
	seedGCChunk(t, c, gcOrphan)
	reached, resume := gcRefreshBarrier(c)
	defer resume()
	done := make(chan error, 1)
	go func() { _, err := c.service.CollectOrphans(context.Background(), gcTestOptions()); done <- err }()
	awaitGCSignal(t, reached)
	owner, _ := gcChunkOwner(gcOrphan)
	// Deliberately bypass service admission to model a metadata change after
	// the snapshot. GetObject must be rechecked, including a new tombstone.
	if err := db.CreateObject(context.Background(), metadata.Object{ID: owner, Deleting: true}); err != nil {
		t.Fatal(err)
	}
	resume()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertGCChunk(t, c, gcOrphan, true)
}

func TestGCDefersNewOrChangedCandidates(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			c, _ := newGCCluster(t)
			seedGCChunk(t, c, gcOrphan)
			reached, resume := gcRefreshBarrier(c)
			defer resume()
			done := make(chan error, 1)
			go func() { _, err := c.service.CollectOrphans(context.Background(), gcTestOptions()); done <- err }()
			awaitGCSignal(t, reached)
			if changed {
				for _, w := range c.workers {
					if err := os.WriteFile(filepath.Join(w.dir, gcOrphan), []byte("changed"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				seedGCChunk(t, c, "obj-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-0")
			}
			resume()
			if err := <-done; err == nil {
				t.Fatal("collected an orphan without a full grace period")
			}
			assertGCChunk(t, c, gcOrphan, true)
			if _, err := c.service.CollectOrphans(context.Background(), gcTestOptions()); err != nil {
				t.Fatal("fresh pass did not recover", err)
			}
			assertGCChunk(t, c, gcOrphan, false)
		})
	}
}

func TestGCFailuresDoNotAuthorizeDeletion(t *testing.T) {
	for _, failure := range []string{"database", "database-recheck", "inventory", "delete", "missing-worker", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			c, db := newGCCluster(t)
			seedGCChunk(t, c, gcOrphan)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			options := gcTestOptions()
			var done chan error
			switch failure {
			case "database":
				_ = db.Close()
			case "database-recheck":
				reached, resume := gcRefreshBarrier(c)
				defer resume()
				done = make(chan error, 1)
				go func() { _, err := c.service.CollectOrphans(ctx, options); done <- err }()
				awaitGCSignal(t, reached)
				_ = db.Close()
				resume()
			case "inventory":
				c.service.client.http.Transport = gcRoundTripper(func(r *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"worker_id":"worker0"}`))}, nil
				})
			case "delete":
				for _, w := range c.workers {
					w.faults.Store(failDelete)
				}
			case "missing-worker":
				c.expire("worker3")
				options.WorkerWaitTimeout = 5 * time.Millisecond
			case "cancel":
				cancel()
			}
			var err error
			if done == nil {
				_, err = c.service.CollectOrphans(ctx, options)
			} else {
				err = <-done
			}
			if err == nil {
				t.Fatal("GC ignored failure")
			}
			assertGCChunk(t, c, gcOrphan, true)
		})
	}
	c := newTestCluster(t)
	if _, err := c.service.CollectOrphans(context.Background(), gcTestOptions()); err == nil {
		t.Fatal("GC accepted volatile metadata")
	}
}

func TestGCCountsAlreadyAbsentWithoutReclaimingBytes(t *testing.T) {
	c, _ := newGCCluster(t)
	seedGCChunk(t, c, gcOrphan)
	c.service.client.http.Transport = gcRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodDelete {
			if err := NewWorkerClient().DeleteChunk(r.Context(), "http://"+r.URL.Host, gcOrphan); err != nil {
				return nil, err
			}
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	stats, err := c.service.CollectOrphans(context.Background(), gcTestOptions())
	if err != nil || stats.Deleted != 0 || stats.EstimatedDeletedBytes != 0 || stats.AlreadyAbsent != 4 {
		t.Fatalf("accounting: %+v %v", stats, err)
	}
}

func TestGCRecoversFailedUploadAfterRestart(t *testing.T) {
	c, db := newGCCluster(t)
	for _, w := range c.workers {
		w.faults.Store(losePutResponse | failDelete)
	}
	_, err := c.service.Upload(context.Background(), "interrupted", "", nil, strings.NewReader("published before metadata commit"))
	if !errors.Is(err, ErrReplicationFailed) || len(c.service.pendingCleanup) != 2 {
		t.Fatalf("expected two uncertain replicas queued for cleanup, err=%v", err)
	}
	path := db.Path()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := metadata.OpenSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	// A restarted controller has no pendingCleanup queue. Only persisted
	// metadata and worker inventories remain as recovery evidence.
	c.service = NewObjectService(c.registry, reopened, NewWorkerClient())
	for _, w := range c.workers {
		w.faults.Store(0)
	}
	stats, err := c.service.CollectOrphans(context.Background(), gcTestOptions())
	if err != nil || stats.Deleted != 2 {
		t.Fatalf("restart recovery: %+v %v", stats, err)
	}
	c.checkEmpty(t)
}

func TestGCMaintenanceExcludesObjectOperations(t *testing.T) {
	c, _ := newGCCluster(t)
	live := c.upload(t, []byte("live"))
	seedGCChunk(t, c, gcOrphan)
	reached, resume := gcRefreshBarrier(c)
	defer resume()
	gcDone := make(chan error, 1)
	go func() { _, err := c.service.CollectOrphans(context.Background(), gcTestOptions()); gcDone <- err }()
	awaitGCSignal(t, reached)
	operations := []func() error{
		func() error {
			_, err := c.service.Upload(context.Background(), "new", "", nil, bytes.NewReader([]byte("new")))
			return err
		},
		func() error { _, err := downloadBytes(c.service, live.ID); return err },
		func() error { _, err := c.service.GetMetadata(context.Background(), live.ID); return err },
		func() error {
			_, err := c.service.PatchMetadata(context.Background(), live.ID, map[string]string{"x": "y"})
			return err
		},
		func() error { _, err := c.service.ListObjects(context.Background()); return err },
		func() error { return c.service.RepairOnce(context.Background()) },
		func() error { return c.service.DeleteObject(context.Background(), live.ID) },
	}
	started := make(chan struct{}, len(operations))
	finished := make(chan error, len(operations))
	for _, operation := range operations {
		go func() { started <- struct{}{}; finished <- operation() }()
	}
	for range operations {
		<-started
	}
	select {
	case err := <-finished:
		t.Fatalf("object operation ran during GC: %v", err)
	default:
	}
	resume()
	if err := <-gcDone; err != nil {
		t.Fatal(err)
	}
	for range operations {
		err := <-finished
		if err != nil && !errors.Is(err, metadata.ErrObjectNotFound) && !errors.Is(err, metadata.ErrObjectDeleting) {
			t.Fatal(err)
		}
	}
	assertGCChunk(t, c, gcOrphan, false)
}

type gcPausedReader struct {
	started chan struct{}
	resume  chan struct{}
	once    sync.Once
	reader  io.Reader
}

func (r *gcPausedReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started); <-r.resume })
	return r.reader.Read(p)
}

func TestGCWaitForActiveUploadIsCancellable(t *testing.T) {
	c, _ := newGCCluster(t)
	reader := &gcPausedReader{started: make(chan struct{}), resume: make(chan struct{}), reader: strings.NewReader("active upload")}
	done := make(chan error, 1)
	go func() { _, err := c.service.Upload(context.Background(), "active", "", nil, reader); done <- err }()
	awaitGCSignal(t, reader.started)
	defer func() {
		close(reader.resume)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := c.service.CollectOrphans(ctx, gcTestOptions()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("maintenance wait ignored cancellation: %v", err)
	}
}

func TestGCChunkOwner(t *testing.T) {
	for _, id := range []string{"legacy", "obj-" + strings.Repeat("A", 32) + "-0", "obj-" + strings.Repeat("g", 32) + "-0", gcOrphan + "/../x", strings.TrimSuffix(gcOrphan, "0") + "01", strings.TrimSuffix(gcOrphan, "0") + "-1"} {
		if _, ok := gcChunkOwner(id); ok {
			t.Fatalf("accepted noncanonical ID %q", id)
		}
	}
	if owner, ok := gcChunkOwner(gcOrphan); !ok || owner != strings.TrimSuffix(gcOrphan, "-0") {
		t.Fatal("rejected canonical ID")
	}
}
