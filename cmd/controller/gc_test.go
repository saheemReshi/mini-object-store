package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"mini-object-store/internal/controller"
	"mini-object-store/internal/metadata"
)

func TestGCRequestParsing(t *testing.T) {
	for _, contents := range []string{"", "  \n", `{}`, `{"mode":"dry-run"}`} {
		options, err := parseGCRequest([]byte(contents))
		if err != nil || options.Mode != "dry-run" || options.GracePeriod != time.Minute || options.WorkerWaitTimeout != time.Minute {
			t.Fatalf("defaults %q: %+v %v", contents, options, err)
		}
	}
	options, err := parseGCRequest([]byte(`{"mode":"delete","grace_period":"2s","worker_wait_timeout":"3s"}`))
	if err != nil || options.Mode != "delete" || options.GracePeriod != 2*time.Second || options.WorkerWaitTimeout != 3*time.Second {
		t.Fatalf("explicit options: %+v %v", options, err)
	}
	for _, contents := range []string{`null`, `[]`, `{"mode":"DELETE"}`, `{"mode":""}`, `{"grace_period":"0s"}`, `{"grace_period":"-1s"}`, `{"worker_wait_timeout":"bad"}`, `{"typo":true}`, `{} {}`, `{`, `{"mode":true}`} {
		if _, err := parseGCRequest([]byte(contents)); err == nil {
			t.Errorf("accepted invalid request %q", contents)
		}
	}
}

func gcRequestEnvironment(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	dbPath, requestPath := filepath.Join(root, "metadata.db"), filepath.Join(root, "gc.request")
	t.Setenv("METADATA_BACKEND", "sqlite")
	t.Setenv("METADATA_DB_PATH", dbPath)
	t.Setenv("GC_REQUEST_PATH", "") // Exercise the default beside the database.
	return dbPath, requestPath
}

func TestStartupGCRequestLifecycle(t *testing.T) {
	dbPath, requestPath := gcRequestEnvironment(t)
	if request, err := prepareStartupGC(); err != nil || request != nil {
		t.Fatalf("ordinary startup requested GC: %v %v", request, err)
	}
	db, err := metadata.OpenSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(requestPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	request, err := prepareStartupGC()
	if err != nil || request.options.Mode != "dry-run" {
		t.Fatalf("empty request: %v %v", request, err)
	}
	// A failed/interrupted run does not call complete, so the next startup sees
	// the same pending request. Merely reading it never consumes authorization.
	if pending, err := prepareStartupGC(); err != nil || pending == nil {
		t.Fatalf("request was consumed early: %v", err)
	}
	if err := request.complete(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(requestPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("completed request still exists", err)
	}
	if pending, err := prepareStartupGC(); err != nil || pending != nil {
		t.Fatal("normal restart repeats GC", err)
	}

	if err := os.WriteFile(requestPath, []byte(`{"mode":"delete"}`), 0600); err != nil {
		t.Fatal(err)
	}
	request, err = prepareStartupGC()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(requestPath, []byte(`{"mode":"dry-run"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := request.complete(); err == nil {
		t.Fatal("removed a changed request")
	}
	if _, err := os.Stat(requestPath); err != nil {
		t.Fatal("changed request was lost", err)
	}
}

func TestStartupGCRejectsUnsafeDatabaseOrRequest(t *testing.T) {
	for _, failure := range []string{"missing-db", "empty-db", "memory", "bad-json", "symlink", "fifo", "directory", "oversize"} {
		t.Run(failure, func(t *testing.T) {
			dbPath, requestPath := gcRequestEnvironment(t)
			if failure != "missing-db" && failure != "empty-db" {
				db, err := metadata.OpenSQLiteStore(dbPath)
				if err != nil {
					t.Fatal(err)
				}
				_ = db.Close()
			}
			switch failure {
			case "symlink":
				if err := os.Symlink(dbPath, requestPath); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := syscall.Mkfifo(requestPath, 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(requestPath, 0700); err != nil {
					t.Fatal(err)
				}
			default:
				contents := `{"mode":"delete"}`
				if failure == "bad-json" {
					contents = "invalid"
				}
				if failure == "oversize" {
					contents = strings.Repeat(" ", 4097)
				}
				if err := os.WriteFile(requestPath, []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "empty-db" {
				if err := os.WriteFile(dbPath, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "memory" {
				t.Setenv("METADATA_BACKEND", "memory")
			}
			if _, err := prepareStartupGC(); err == nil {
				t.Fatal("accepted unsafe GC request")
			}
			if _, err := os.Lstat(requestPath); err != nil {
				t.Fatal("lost failed request", err)
			}
			if failure == "missing-db" {
				if _, err := os.Stat(dbPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("created missing database before validating GC", err)
				}
			}
		})
	}
}

func TestStartupGCLeavesInvalidDatabaseDiagnosticToOpenStore(t *testing.T) {
	_, _ = gcRequestEnvironment(t)
	parent := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(parent, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("METADATA_DB_PATH", filepath.Join(parent, "metadata.db"))
	if request, err := prepareStartupGC(); request != nil || err != nil {
		t.Fatalf("implicit absent request masked the database error: %v %v", request, err)
	}
	if _, _, err := openStore(); err == nil || !strings.Contains(err.Error(), "METADATA_DB_PATH") {
		t.Fatalf("lost database startup diagnostic: %v", err)
	}
}

func TestStartupGateKeepsMembershipAvailable(t *testing.T) {
	registry := controller.NewRegistry()
	service := controller.NewObjectService(registry, metadata.NewMemoryStore(), controller.NewWorkerClient())
	var ready atomic.Bool
	handler := withStartupGate(controller.NewHandler(registry, service), &ready)
	var wg sync.WaitGroup
	for i := 0; i < 36; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, route := range []struct{ method, path string }{
				{"POST", "/api/v1/objects"}, {"GET", "/api/v1/objects"},
				{"GET", "/api/v1/objects/missing"}, {"DELETE", "/api/v1/objects/missing"},
				{"GET", "/api/v1/objects/missing/metadata"}, {"PATCH", "/api/v1/objects/missing/metadata"},
			} {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(route.method, route.path, nil))
				var body map[string]string
				if response.Code != 503 || response.Header().Get("Retry-After") == "" || response.Header().Get("Content-Type") != "application/json" || json.Unmarshal(response.Body.Bytes(), &body) != nil || body["error"] == "" {
					t.Errorf("maintenance response: %d %v %s", response.Code, response.Header(), response.Body)
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("worker%d", i)
		for _, req := range []struct{ path, body string }{
			{"/internal/workers/register", fmt.Sprintf(`{"worker_id":%q,"address":%q}`, id, fmt.Sprintf("http://127.0.0.1:%d", 9000+i))},
			{"/internal/workers/heartbeat", fmt.Sprintf(`{"worker_id":%q}`, id)},
		} {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("POST", req.path, bytes.NewBufferString(req.body)))
			if response.Code >= 300 {
				t.Fatalf("membership blocked: %d %s", response.Code, response.Body)
			}
		}
	}
	wg.Wait()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/internal/workers", nil))
	if response.Code != 200 || len(registry.List()) != 4 {
		t.Fatal("health/membership unavailable")
	}
	ready.Store(true)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/objects", nil))
	if response.Code != 200 {
		t.Fatal("object API did not resume", response.Code)
	}
}
