package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestInvalidChunksNeverPublish(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, id string
		size     int64
		data     []byte
		hash     [32]byte
	}{
		{"path", "../escape", 1, []byte("a"), sha256.Sum256([]byte("a"))},
		{"empty", "empty", 0, nil, sha256.Sum256(nil)},
		{"large", "large", ChunkSize + 1, nil, sha256.Sum256(nil)},
		{"short", "short", 2, []byte("a"), sha256.Sum256([]byte("a"))},
		{"long", "long", 1, []byte("ab"), sha256.Sum256([]byte("ab"))},
		{"checksum", "hash", 1, []byte("a"), sha256.Sum256([]byte("b"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := s.Put(tc.id, bytes.NewReader(tc.data), tc.size, tc.hash); !errors.Is(err, ErrInvalidChunk) {
				t.Fatalf("Put: %v", err)
			}
			entries, err := os.ReadDir(s.rootDir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("invalid write left files: %v, %v", entries, err)
			}
		})
	}
}

func TestRejectSymlinksAndSpecialFiles(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "symlink")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"symlink", "fifo", "directory"} {
		if f, err := s.Get(id); err == nil {
			f.Close()
			t.Fatalf("Get accepted %s", id)
		}
		if err := s.Put(id, bytes.NewReader([]byte("secret")), 6, sha256.Sum256([]byte("secret"))); err == nil {
			t.Fatalf("retry accepted %s", id)
		}
	}
	listed, err := s.List()
	if err != nil || len(listed) != 0 {
		t.Fatalf("listed unsafe files: %+v, %v", listed, err)
	}
}

type blockingReader struct {
	started, resume chan struct{}
	data            *bytes.Reader
}

func (r *blockingReader) Read(p []byte) (int, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
		<-r.resume
	}
	return r.data.Read(p)
}

func TestPutDeleteSerializationAndWaitingLockLifetime(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := &blockingReader{started: make(chan struct{}), resume: make(chan struct{}), data: bytes.NewReader([]byte("data"))}
	putDone := make(chan error, 1)
	go func() { putDone <- s.Put("chunk", r, 4, sha256.Sum256([]byte("data"))) }()
	<-r.started
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- s.Delete("chunk") }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.locksMu.Lock()
		refs := s.chunkLocks["chunk"].refs
		s.locksMu.Unlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			close(r.resume)
			t.Fatal("DELETE did not retain its waiting lock reference")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-deleteDone:
		close(r.resume)
		t.Fatalf("DELETE overtook staged PUT: %v", err)
	default:
	}
	close(r.resume)
	if err := <-putDone; err != nil {
		t.Fatal(err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("chunk"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("chunk resurrected: %v", err)
	}
	if len(s.chunkLocks) != 0 {
		t.Fatal("lock registry leaked")
	}
}

type cancelReader struct {
	cancel context.CancelFunc
	done   bool
}

func (r *cancelReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	p[0] = 'x'
	r.cancel()
	return 1, io.EOF
}

func TestCanceledPutDoesNotPublish(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = s.PutContext(ctx, "canceled", &cancelReader{cancel: cancel}, 1, sha256.Sum256([]byte("x")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PutContext: %v", err)
	}
	entries, err := os.ReadDir(s.rootDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("cancellation leaked files: %v, %v", entries, err)
	}
}

func TestConcurrentChunkOperations(t *testing.T) {
	s, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("immutable chunk snapshot")
	digest := sha256.Sum256(data)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			id := fmt.Sprintf("chunk-%d", g%4)
			for i := 0; i < 20; i++ {
				if err := s.Put(id, bytes.NewReader(data), int64(len(data)), digest); err != nil {
					t.Error(err)
					return
				}
				f, err := s.Get(id)
				if err == nil {
					b, readErr := io.ReadAll(f)
					f.Close()
					if readErr != nil || !bytes.Equal(b, data) {
						t.Errorf("partial snapshot: %q, %v", b, readErr)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Error(err)
				}
				if err := s.Delete(id); err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Error(err)
				}
				if _, err := s.List(); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
	if len(s.chunkLocks) != 0 {
		t.Fatal("lock registry leaked after concurrent operations")
	}
}
