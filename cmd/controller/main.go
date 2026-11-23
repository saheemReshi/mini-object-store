package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"mini-object-store/internal/controller"
	"mini-object-store/internal/metadata"
)

const defaultMetadataDBPath = "./data/controller/metadata.db"

func main() {
	if err := run(); err != nil {
		log.Printf("controller: %v", err)
		os.Exit(1)
	}
}

// openStore selects the metadata backend. SQLite is the default; the in-memory
// store must be requested explicitly and is never used as a silent fallback.
func openStore() (metadata.Store, io.Closer, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("METADATA_BACKEND")))
	switch backend {
	case "", "sqlite":
		path := metadataDBPath()
		store, err := metadata.OpenSQLiteStore(path)
		if err != nil {
			return nil, nil, fmt.Errorf("open metadata database (METADATA_DB_PATH=%q): %w", path, err)
		}
		log.Printf("metadata: SQLite database %s", path)
		return store, store, nil
	case "memory":
		log.Printf("metadata: IN-MEMORY store; metadata is lost when the controller exits")
		return metadata.NewMemoryStore(), nopCloser{}, nil
	default:
		return nil, nil, fmt.Errorf("unknown METADATA_BACKEND %q (use sqlite or memory)", backend)
	}
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func shutdownServer(ctx context.Context, server *http.Server) error {
	if err := server.Shutdown(ctx); err != nil {
		// Shutdown stops waiting when ctx expires but leaves active connections
		// open. Close them to cancel request contexts and release object locks
		// before run waits for a repair goroutine blocked behind those requests.
		return errors.Join(err, server.Close())
	}
	return nil
}

func run() error {
	addr := os.Getenv("CONTROLLER_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	request, err := prepareStartupGC()
	if err != nil {
		return err
	}
	metaStore, closer, err := openStore()
	if err != nil {
		return err
	}
	defer func() {
		if err := closer.Close(); err != nil {
			log.Printf("close metadata store: %v", err)
		}
	}()

	// SIGTERM is what `docker stop` sends; SIGINT is Ctrl+C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := controller.NewRegistry()
	workerClient := controller.NewWorkerClient()

	objectService := controller.NewObjectService(
		registry,
		metaStore,
		workerClient,
	)

	// No client operation or repair can begin before requested maintenance.
	var ready atomic.Bool
	ready.Store(request == nil)
	var background sync.WaitGroup
	background.Add(1)
	maintenanceErr := make(chan error, 1)
	go func() {
		defer background.Done()
		if request != nil {
			log.Printf("gc: requested mode=%s path=%s", request.options.Mode, request.path)
			if _, err := objectService.CollectOrphans(ctx, request.options); err != nil {
				maintenanceErr <- fmt.Errorf("startup GC failed (request retained): %w", err)
				return
			}
			if ctx.Err() != nil {
				return
			}
			if err := request.complete(); err != nil {
				maintenanceErr <- fmt.Errorf("complete GC request: %w", err)
				return
			}
			ready.Store(true)
			log.Printf("gc: complete; request removed; object API ready")
		}
		objectService.RunRepair(ctx, 15*time.Second)
	}()

	server := &http.Server{
		Addr:              addr,
		Handler:           withStartupGate(controller.NewHandler(registry, objectService), &ready),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("controller listening on %s", addr)
		serveErr <- server.ListenAndServe()
	}()

	select {
	case err = <-serveErr:
		// Listener failed (for example, port in use).
	case err = <-maintenanceErr:
		// Keep the request and stop rather than open client traffic after failure.
	case <-ctx.Done():
	}
	if ctx.Err() != nil && errors.Is(err, context.Canceled) {
		err = nil
	}
	stop() // Cancel GC timers/RPCs and repair before closing the store.
	log.Printf("controller shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	shutdownErr := shutdownServer(shutdownCtx, server)
	cancel()
	background.Wait()

	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return errors.Join(err, shutdownErr)
}
