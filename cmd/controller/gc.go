package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"mini-object-store/internal/controller"
)

type startupGC struct {
	options  controller.GCOptions
	path     string
	contents []byte
	info     os.FileInfo
}

func metadataDBPath() string {
	if path := os.Getenv("METADATA_DB_PATH"); path != "" {
		return path
	}
	return defaultMetadataDBPath
}

// prepareStartupGC runs before openStore can create a missing database. A
// freshly recreated database is not authority to erase existing worker data.
func prepareStartupGC() (*startupGC, error) {
	path := os.Getenv("GC_REQUEST_PATH")
	if path == "" {
		path = filepath.Join(filepath.Dir(metadataDBPath()), "gc.request")
	}
	contents, info, err := readGCRequest(path)
	// An implicit request cannot exist beneath an invalid database parent.
	// Let openStore report the database configuration error in that case.
	if errors.Is(err, os.ErrNotExist) || (os.Getenv("GC_REQUEST_PATH") == "" && errors.Is(err, syscall.ENOTDIR)) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read GC request %q: %w", path, err)
	}
	options, err := parseGCRequest(contents)
	if err != nil {
		return nil, fmt.Errorf("invalid GC request %q: %w", path, err)
	}
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("METADATA_BACKEND")))
	if backend != "" && backend != "sqlite" {
		return nil, errors.New("requested GC requires the SQLite backend")
	}
	// A valid header prevents an existing empty placeholder from being silently
	// initialized and used as an empty manifest. Full validation is openStore's
	// responsibility. Operators must still choose the correct database volume.
	db, err := os.OpenFile(metadataDBPath(), os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("GC requires an existing metadata database: %w", err)
	}
	defer db.Close()
	if info, err := db.Stat(); err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("GC metadata database must be an existing regular file")
	}
	var header [16]byte
	if _, err := io.ReadFull(db, header[:]); err != nil || string(header[:]) != "SQLite format 3\x00" {
		return nil, errors.New("GC requires an existing SQLite metadata database; request retained")
	}
	return &startupGC{options: options, path: path, contents: contents, info: info}, nil
}

func parseGCRequest(contents []byte) (controller.GCOptions, error) {
	wire := struct {
		Mode              string `json:"mode"`
		GracePeriod       string `json:"grace_period"`
		WorkerWaitTimeout string `json:"worker_wait_timeout"`
	}{Mode: "dry-run", GracePeriod: "60s", WorkerWaitTimeout: "60s"}
	trimmed := bytes.TrimSpace(contents)
	if len(trimmed) != 0 {
		if trimmed[0] != '{' {
			return controller.GCOptions{}, errors.New("expected a JSON object or an empty dry-run request")
		}
		decoder := json.NewDecoder(bytes.NewReader(contents))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&wire); err != nil {
			return controller.GCOptions{}, err
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return controller.GCOptions{}, errors.New("expected exactly one JSON object")
		}
	}
	grace, graceErr := time.ParseDuration(wire.GracePeriod)
	wait, waitErr := time.ParseDuration(wire.WorkerWaitTimeout)
	if (wire.Mode != "dry-run" && wire.Mode != "delete") || graceErr != nil || waitErr != nil || grace <= 0 || wait <= 0 {
		return controller.GCOptions{}, errors.New("mode must be dry-run or delete; durations must be positive Go durations")
	}
	return controller.GCOptions{Mode: wire.Mode, GracePeriod: grace, WorkerWaitTimeout: wait}, nil
}

func readGCRequest(path string) ([]byte, os.FileInfo, error) {
	// The request is an operator-owned regular file, never a symlink, FIFO,
	// directory, or device. O_NONBLOCK prevents a bad FIFO from hanging startup.
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, errors.New("GC request must be a regular file")
	}
	const limit = 4096
	contents, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, nil, err
	}
	if len(contents) > limit {
		return nil, nil, errors.New("GC request exceeds 4096 bytes")
	}
	return contents, info, nil
}

func (request *startupGC) complete() error {
	contents, info, err := readGCRequest(request.path)
	if err != nil {
		return err
	}
	if !os.SameFile(info, request.info) || !bytes.Equal(contents, request.contents) {
		return errors.New("GC request changed during maintenance; new request retained")
	}
	if err := os.Remove(request.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(request.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// Only client routes are gated. The membership endpoint used by Docker's
// healthcheck stays available so depends_on does not deadlock worker startup.
func withStartupGate(next http.Handler, ready *atomic.Bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() && (r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/")) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "startup garbage collection in progress; retry later"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
